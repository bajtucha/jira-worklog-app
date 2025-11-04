// server.js — hybrid sync (worklog-first + JQL fallback)
import express from 'express';
import axios from 'axios';
import dotenv from 'dotenv';
import cron from 'node-cron';
import Database from 'better-sqlite3';
import { stringify } from 'csv-stringify';
import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream';
import { promisify } from 'util';
import crypto from 'crypto';

const pipe = promisify(pipeline);
dotenv.config();

/* ================= helpers ================= */
function parseEnvGroups(str) {
  if (!str) return [];
  return str
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(entry => {
      const [label, name] = entry.split('=').map(x => x.trim());
      if (!label || !name) return null;
      return { label, name };
    })
    .filter(Boolean);
}

// "Booking Engine=mail1;mail2|Checkout=mail3;mail4"
function parseGroupOverrides(str) {
  const map = new Map();
  if (!str) return map;
  const groups = str.split('|').map(s => s.trim()).filter(Boolean);
  for (const g of groups) {
    const [label, list] = g.split('=').map(x => x.trim());
    if (!label || !list) continue;
    const emails = list.split(';').map(x => x.trim()).filter(Boolean);
    map.set(label.toLowerCase(), emails);
  }
  return map;
}

// tiny ADF -> plain text
function adfToPlain(adf) {
  try {
    if (!adf || typeof adf !== 'object') return null;
    const out = [];
    const walk = (node) => {
      if (!node || typeof node !== 'object') return;
      if (node.type === 'text') { out.push(node.text || ''); return; }
      if (node.type === 'hardBreak') { out.push('\n'); return; }
      if (Array.isArray(node.content)) node.content.forEach(walk);
    };
    walk(adf);
    const txt = out.join('').replace(/\s+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
    return txt || null;
  } catch { return null; }
}

const {
  JIRA_BASE_URL,
  JIRA_EMAIL,
  JIRA_API_TOKEN,
  JQL,
  CRON_SCHEDULE = '*/30 * * * *',
  PORT = 8080,
  JIRA_GROUPS,
  JIRA_GROUP_OVERRIDES,
  JIRA_PROJECTS,
  FAST_MODE = 'true',
  SEARCH_CHUNK_DAYS: SEARCH_CHUNK_DAYS_ENV = '5'
} = process.env;

const GROUPS = parseEnvGroups(JIRA_GROUPS || '');
const GROUP_OVERRIDES = parseGroupOverrides(JIRA_GROUP_OVERRIDES || '');
const FAST = String(FAST_MODE).toLowerCase() === 'true';
const SEARCH_CHUNK_DAYS = Number(SEARCH_CHUNK_DAYS_ENV || 5);

if (!JIRA_BASE_URL || !JIRA_EMAIL || !JIRA_API_TOKEN) {
  console.error('❌ Missing JIRA_* env vars. Check .env.');
  process.exit(1);
}

/* ================= FS cache (CSV) ================= */
const CACHE_DIR = path.resolve('cache', 'reports');
fs.mkdirSync(CACHE_DIR, { recursive: true });

function slug(s) {
  return String(s || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}
function csvKey({ from, to, group }) {
  return `${slug(group)}__${from}__${to}.csv`;
}
function csvPathFor(q) {
  return path.join(CACHE_DIR, csvKey(q));
}
async function fileExists(fp) {
  try { await fs.promises.access(fp, fs.constants.F_OK); return true; }
  catch { return false; }
}
function addDays(iso, days){
  const d = new Date(iso + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0,10);
}
function minIso(a,b){ return (a<b)?a:b; }
function maxIso(a,b){ return (a>b)?a:b; }

/* ================= DB setup ================= */
const db = new Database('data.db');
db.pragma('journal_mode = WAL');

db.exec(`
CREATE TABLE IF NOT EXISTS issues (
  id TEXT PRIMARY KEY,
  key TEXT,
  summary TEXT,
  issue_type TEXT,
  priority TEXT,
  status TEXT,
  assignee_email TEXT,
  epic_key TEXT,
  sprint TEXT,
  updated_at TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS worklogs (
  id TEXT PRIMARY KEY,
  issue_id TEXT,
  author_email TEXT,
  time_spent_seconds INTEGER,
  started TEXT,
  created TEXT,
  updated TEXT,
  comment TEXT,
  author_account_id TEXT,
  FOREIGN KEY(issue_id) REFERENCES issues(id)
);

CREATE INDEX IF NOT EXISTS idx_worklogs_started ON worklogs(started);
CREATE INDEX IF NOT EXISTS idx_worklogs_author  ON worklogs(author_email);
CREATE INDEX IF NOT EXISTS idx_worklogs_author_id ON worklogs(author_account_id);
`);
function columnExists(table, col) {
  try {
    const rows = db.prepare(`PRAGMA table_info(${table})`).all();
    return rows.some(r => r.name === col);
  } catch { return false; }
}
if (!columnExists('issues', 'description')) {
  try { db.exec(`ALTER TABLE issues ADD COLUMN description TEXT`); } catch {}
}

const upsertIssueStmt = db.prepare(`
INSERT INTO issues (id, key, summary, issue_type, priority, status, assignee_email, epic_key, sprint, updated_at, description)
VALUES (@id, @key, @summary, @issue_type, @priority, @status, @assignee_email, @epic_key, @sprint, @updated_at, @description)
ON CONFLICT(id) DO UPDATE SET
  key=excluded.key,
  summary=excluded.summary,
  issue_type=excluded.issue_type,
  priority=excluded.priority,
  status=excluded.status,
  assignee_email=excluded.assignee_email,
  epic_key=excluded.epic_key,
  sprint=excluded.sprint,
  updated_at=excluded.updated_at,
  description=excluded.description
`);

const upsertWorklogStmt = db.prepare(`
INSERT INTO worklogs (id, issue_id, author_email, time_spent_seconds, started, created, updated, comment, author_account_id)
VALUES (@id, @issue_id, @author_email, @time_spent_seconds, @started, @created, @updated, @comment, @author_account_id)
ON CONFLICT(id) DO UPDATE SET
  issue_id=excluded.issue_id,
  author_email=excluded.author_email,
  author_account_id=excluded.author_account_id,
  time_spent_seconds=excluded.time_spent_seconds,
  started=excluded.started,
  created=excluded.created,
  updated=excluded.updated,
  comment=excluded.comment
`);

/* ================= Jira client ================= */
const jira = axios.create({
  baseURL: `${JIRA_BASE_URL}/rest/api/3`,
  headers: {
    Accept: 'application/json',
    Authorization: 'Basic ' + Buffer.from(`${JIRA_EMAIL}:${JIRA_API_TOKEN}`).toString('base64')
  },
  timeout: 60000
});

/* ================= group helpers ================= */
async function resolveGroupIdByName(groupName) {
  try {
    const { data } = await jira.get('/group', { params: { groupname: groupName } });
    return data?.groupId || data?.id || null;
  } catch (e) {
    // brak grupy / brak uprawnień / 4xx: nie wysadzamy procesu
    console.warn('resolveGroupIdByName failed for:', groupName, e?.response?.status, e?.response?.data);
    return null;
  }
}

const groupCache = new Map(); // groupName -> {expires, members}
async function getGroupMembersFromJira(groupName) {
  try {
    const groupId = await resolveGroupIdByName(groupName);
    if (!groupId) return [];
    let startAt = 0;
    const maxResults = 50;
    const members = [];
    while (true) {
      const { data } = await jira.get('/group/member', { params: { groupId, startAt, maxResults } });
      const values = data?.values || [];
      for (const u of values) {
        members.push({
          accountId: u.accountId,
          emailAddress: u.emailAddress || null,
          displayName: u.displayName || null
        });
      }
      if (data?.isLast || startAt + maxResults >= (data?.total ?? values.length)) break;
      startAt += maxResults;
    }
    return members;
  } catch (e) {
    console.warn('getGroupMembersFromJira failed for:', groupName, e?.response?.status, e?.response?.data);
    return [];
  }
}

async function getGroupMembersMerged(label, apiName) {
  const now = Date.now();
  const cacheKey = `L=${label}|N=${apiName}`;
  const cached = groupCache.get(cacheKey);
  if (cached && cached.expires > now) return cached.members;

  let apiMembers = [];
  try { apiMembers = await getGroupMembersFromJira(apiName); } 
  catch (e) { 
    console.warn('getGroupMembersMerged api error:', e?.message);
    apiMembers = [];
  }

  const overrideEmails = GROUP_OVERRIDES.get(label.toLowerCase()) || [];
  const overrideMembers = overrideEmails.map(email => ({ accountId: null, emailAddress: email, displayName: email }));

  const byKey = new Map();
  for (const m of [...apiMembers, ...overrideMembers]) {
    const key = m.accountId || (m.emailAddress ? `mail:${m.emailAddress.toLowerCase()}` : null);
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, m);
  }
  const merged = [...byKey.values()];

  groupCache.set(cacheKey, { expires: now + 5 * 60 * 1000, members: merged });
  return merged;
}


/* =================== worklog-first (primary) =================== */
// 1) /worklog/updated → set of IDs (with safety margins)
async function fetchWorklogIdsUpdated(fromIso, toIso) {
  const safetyFrom = addDays(fromIso, -2);
  const safetyTo   = addDays(toIso,   2);

  let since = new Date(`${safetyFrom}T00:00:00Z`).getTime();
  const stopAt = new Date(`${safetyTo}T23:59:59Z`).getTime();

  const ids = new Set();
  while (since <= stopAt) {
    const { data } = await jira.get('/worklog/updated', { params: { since } }).catch(() => ({ data: null }));
    if (!data) break;
    const values = Array.isArray(data.values) ? data.values : [];
    for (const v of values) {
      if (typeof v.worklogId === 'number') ids.add(String(v.worklogId));
    }
    const next = (typeof data.until === 'number') ? data.until : (since + 60_000);
    if (next <= since) break;
    since = next;
    if (values.length === 0 && since < stopAt) since += 3600_000; // nudge 1h
  }
  return Array.from(ids);
}

// 2) `/worklog/list` batched + filters (date started + group)
async function fetchWorklogsDetailedFiltered(worklogIds, fromIso, toIso, groupLabel) {
  if (!worklogIds.length) return [];
  const g = GROUPS.find(x =>
    x.label.toLowerCase() === groupLabel.toLowerCase() ||
    x.name.toLowerCase()  === groupLabel.toLowerCase()
  );
  const groupName = g?.name || groupLabel;
  const members = await getGroupMembersMerged(g?.label || groupLabel, groupName).catch(()=>[]);
  const idsSet = new Set(members.map(m => (m.accountId || '').toLowerCase()).filter(Boolean));
  const mailsSet = new Set(members.map(m => (m.emailAddress || '').toLowerCase()).filter(Boolean));

  const inRange = (startedStr) => {
    if (!startedStr) return false;
    const day = String(startedStr).slice(0,10);
    return (day >= fromIso && day <= toIso);
  };

  const batchSize = 1000;
  const out = [];
  for (let i=0;i<worklogIds.length;i+=batchSize){
    const chunk = worklogIds.slice(i, i+batchSize).map(id => Number(id)).filter(n => Number.isFinite(n));
    if (!chunk.length) continue;
    const { data } = await jira.post('/worklog/list', { ids: chunk }).catch(()=>({ data: [] }));
    const list = Array.isArray(data) ? data : [];
    for (const w of list) {
      const started = w.started || null;
      if (!inRange(started)) continue;
      const accountId = w.author?.accountId ? String(w.author.accountId).toLowerCase() : null;
      const email     = w.author?.emailAddress ? String(w.author.emailAddress).toLowerCase() : null;
      if (idsSet.size || mailsSet.size) {
        const ok = (accountId && idsSet.has(accountId)) || (email && mailsSet.has(email));
        if (!ok) continue;
      }
      let comment = w.comment;
      if (comment && typeof comment !== 'string') {
        try { comment = JSON.stringify(comment); } catch { comment = null; }
      }
      out.push({
        id: String(w.id),
        issue_id: String(w.issueId),
        author_email: email,
        author_account_id: accountId,
        time_spent_seconds: w.timeSpentSeconds || 0,
        started: started,
        created: w.created || null,
        updated: w.updated || null,
        comment: comment || null
      });
    }
  }
  return out;
}

// 3) pull issues for collected issueIds
async function fetchIssuesDetails(issueIds) {
  if (!issueIds.length) return [];
  const fields = [
    'summary','issuetype','priority','status','assignee','parent',
    'customfield_10008','customfield_10014','updated','description'
  ];
  const batchSize = 800;
  const out = [];
  for (let i=0;i<issueIds.length;i+=batchSize) {
    const chunk = issueIds.slice(i, i+batchSize);
    const jql = `id in (${chunk.join(',')})`;
    const body = { jql, startAt: 0, maxResults: 1000, fields };
    const { data } = await jira.post('/search', body).catch(()=>({ data: { issues: [] }}));
    const issues = Array.isArray(data.issues) ? data.issues : [];
    out.push(...issues);
  }
  return out;
}
// ==== make sure all referenced issues exist in DB ====
function setDiff(aSet, bSet) { const out = new Set(aSet); for (const x of bSet) out.delete(x); return out; }

function getExistingIssueIds(ids) {
  if (!ids.length) return new Set();
  const params = Object.fromEntries(ids.map((v,i)=>['id'+i, String(v)]));
  const ph = ids.map((_,i)=>'@id'+i).join(',');
  const rows = db.prepare(`SELECT id FROM issues WHERE id IN (${ph})`).all(params);
  return new Set(rows.map(r => String(r.id)));
}

async function ensureIssuesInDb(issueIds) {
  const uniq = Array.from(new Set(issueIds.map(String))).filter(Boolean);
  if (!uniq.length) return { fetched: 0, existed: 0 };

  const existing = getExistingIssueIds(uniq);
  const missing = Array.from(setDiff(new Set(uniq), existing));
  if (!missing.length) return { fetched: 0, existed: existing.size };

  const fetched = await fetchIssuesDetails(missing); // już masz tę funkcję
  const trx = db.transaction(() => {
    for (const it of fetched) {
      const f = it.fields || {};
      const sprintRaw = f['customfield_10008'];
      const lastSprint = Array.isArray(sprintRaw) && sprintRaw.length ? (sprintRaw.at(-1)?.name || sprintRaw.at(-1)) : null;
      const epicKey = f['customfield_10014'] || (f.parent && f.parent.key?.startsWith('EPIC') ? f.parent.key : null);
      const descPlain = typeof f.description === 'string' ? f.description
                        : (f.description && typeof f.description === 'object' ? adfToPlain(f.description) : null);
      upsertIssueStmt.run({
        id: it.id,
        key: it.key,
        summary: f.summary || null,
        issue_type: f.issuetype?.name || null,
        priority: f.priority?.name || null,
        status: f.status?.name || null,
        assignee_email: f.assignee?.emailAddress || null,
        epic_key: epicKey || null,
        sprint: lastSprint || null,
        updated_at: f.updated || null,
        description: descPlain || null
      });
    }
  });
  trx();

  return { fetched: fetched.length, existed: existing.size };
}

/* =================== search fallback (history) =================== */
async function pagedSearch(jql, fields = [], pageSize = 100) {
  const all = [];
  let startAt = 0;
  while (true) {
    const body = { jql, startAt, maxResults: pageSize, fields: fields.length ? fields : undefined };
    const { data } = await jira.post('/search', body).catch(()=>({ data:{ issues:[], total:0 }}));
    const issues = data.issues || [];
    all.push(...issues);
    const total = Number.isFinite(data.total) ? data.total : issues.length;
    startAt += issues.length;
    if (startAt >= total || issues.length === 0) break;
  }
  return all;
}
async function getIssueWorklogs(issueId) {
  const pageSize = 100;
  let startAt = 0;
  let total = Infinity;
  const all = [];
  while (startAt < total) {
    const res = await jira.get(`/issue/${issueId}/worklog`, { params: { startAt, maxResults: pageSize } });
    const data = res.data || {};
    const items = Array.isArray(data.worklogs) ? data.worklogs : [];
    all.push(...items);
    total = typeof data.total === 'number' ? data.total : (startAt + items.length);
    startAt += items.length;
    if (items.length === 0) break;
  }
  for (const w of all) {
    if (w && w.comment && typeof w.comment !== 'string') {
      try { w.comment = JSON.stringify(w.comment); } catch { w.comment = null; }
    }
  }
  return all;
}
function buildProjectsFilter() {
  const projects = (JIRA_PROJECTS || '').split(',').map(s => s.trim()).filter(Boolean);
  return projects.length ? ` AND project in (${projects.map(p => `"${p}"`).join(',')})` : '';
}
async function syncSearchFallback({ from, to, groupLabel }) {
  const g = GROUPS.find(x =>
    x.label.toLowerCase() === groupLabel.toLowerCase() ||
    x.name.toLowerCase()  === groupLabel.toLowerCase()
  );
  const groupName = g?.name || groupLabel;

  const pf = buildProjectsFilter();
  const fields = [
    'summary','issuetype','priority','status','assignee','parent',
    'customfield_10008','customfield_10014','updated','description'
  ];

  const uniqueIssues = new Map();
  // poszerzamy okno wyszukiwania minimalnie
  const searchFrom = addDays(from, -1);
  const searchTo   = addDays(to,   1);

  for (let windowStart = searchFrom; windowStart <= searchTo; windowStart = addDays(windowStart, SEARCH_CHUNK_DAYS)) {
    const windowEnd = minIso(addDays(windowStart, SEARCH_CHUNK_DAYS - 1), searchTo);

    const baseJql = `worklogDate >= "${windowStart}" AND worklogDate <= "${windowEnd}"${pf}`;
    const jqlWithMembers = `${baseJql} AND worklogAuthor in membersOf("${groupName.replace(/"/g,'\\"')}")`;

    let chunkIssues = [];
    try { chunkIssues = await pagedSearch(jqlWithMembers, fields); } catch {}
    if (chunkIssues.length === 0) {
      try { chunkIssues = await pagedSearch(baseJql, fields); } catch {}
    }
    for (const it of chunkIssues) if (!uniqueIssues.has(it.id)) uniqueIssues.set(it.id, it);
  }

  const issues = Array.from(uniqueIssues.values());

  const trxI = db.transaction(() => {
    for (const it of issues) {
      const f = it.fields || {};
      const sprintRaw = f['customfield_10008'];
      const lastSprint = Array.isArray(sprintRaw) && sprintRaw.length ? (sprintRaw.at(-1)?.name || sprintRaw.at(-1)) : null;
      const epicKey = f['customfield_10014'] || (f.parent && f.parent.key?.startsWith('EPIC') ? f.parent.key : null);
      const descPlain = typeof f.description === 'string' ? f.description
                        : (f.description && typeof f.description === 'object' ? adfToPlain(f.description) : null);
      upsertIssueStmt.run({
        id: it.id,
        key: it.key,
        summary: f.summary || null,
        issue_type: f.issuetype?.name || null,
        priority: f.priority?.name || null,
        status: f.status?.name || null,
        assignee_email: f.assignee?.emailAddress || null,
        epic_key: epicKey || null,
        sprint: lastSprint || null,
        updated_at: f.updated || null,
        description: descPlain || null
      });
    }
  });
  trxI();

  let insertedW = 0;
  for (const it of issues) {
    let logs = [];
    try { logs = await getIssueWorklogs(it.id); } catch {}
    const trxW = db.transaction(() => {
      for (const w of logs) {
        upsertWorklogStmt.run({
          id: w.id,
          issue_id: it.id,
          author_email: w.author?.emailAddress || null,
          author_account_id: w.author?.accountId || null,
          time_spent_seconds: w.timeSpentSeconds || 0,
          started: w.started || null,
          created: w.created || null,
          updated: w.updated || null,
          comment: typeof w.comment === 'string' ? w.comment : null
        });
      }
    });
    trxW();
    insertedW += logs.length;
  }

  return { issues: issues.length, worklogs: insertedW, retried: true, mode: 'search-fallback' };
}

/* =================== hybrid driver =================== */
async function syncWorklogsForRangeAndGroup({ from, to, groupLabel }) {
  // 1) worklog-first
    // 1) worklog-first
  const wlIds = await fetchWorklogIdsUpdated(from, to);
  if (wlIds.length) {
    const logs = await fetchWorklogsDetailedFiltered(wlIds, from, to, groupLabel);
    if (logs.length) {
      const issueIds = Array.from(new Set(logs.map(w => w.issue_id))).filter(Boolean);

      // 🔒 upewnij się, że wszystkie issues istnieją (inaczej FK poleci)
      const ensured = await ensureIssuesInDb(issueIds);

      // ⬇︎ zapisz worklogi z łagodnym try/catch na każdy rekord
      let inserted = 0, skipped = 0;
      const trxW = db.transaction(() => {
        for (const w of logs) {
          try {
            upsertWorklogStmt.run({
              id: w.id,
              issue_id: w.issue_id,
              author_email: w.author_email || null,
              author_account_id: w.author_account_id || null,
              time_spent_seconds: w.time_spent_seconds || 0,
              started: w.started || null,
              created: w.created || null,
              updated: w.updated || null,
              comment: w.comment || null
            });
            inserted++;
          } catch (e) {
            skipped++;
          }
        }
      });
      trxW();

      // dla spójności dociągamy metadane issues, ale ensureIssuesInDb zwykle już to zrobił
      const issues = await fetchIssuesDetails(issueIds);

      // upsert issues (idempotentnie)
      const trxI = db.transaction(() => {
        for (const it of issues) {
          const f = it.fields || {};
          const sprintRaw = f['customfield_10008'];
          const lastSprint = Array.isArray(sprintRaw) && sprintRaw.length ? (sprintRaw.at(-1)?.name || sprintRaw.at(-1)) : null;
          const epicKey = f['customfield_10014'] || (f.parent && f.parent.key?.startsWith('EPIC') ? f.parent.key : null);
          const descPlain = typeof f.description === 'string' ? f.description
                            : (f.description && typeof f.description === 'object' ? adfToPlain(f.description) : null);
          upsertIssueStmt.run({
            id: it.id,
            key: it.key,
            summary: f.summary || null,
            issue_type: f.issuetype?.name || null,
            priority: f.priority?.name || null,
            status: f.status?.name || null,
            assignee_email: f.assignee?.emailAddress || null,
            epic_key: epicKey || null,
            sprint: lastSprint || null,
            updated_at: f.updated || null,
            description: descPlain || null
          });
        }
      });
      trxI();

      return {
        issues: issues.length,
        worklogs: inserted,
        retried: false,
        mode: 'worklog-first',
        skippedWorklogs: skipped,
        ensuredIssuesFetched: ensured.fetched
      };
    }
  }


  // 2) history fallback
  return await syncSearchFallback({ from, to, groupLabel });
}

/* ================= CSV build/read & SQL ================= */
const CSV_COLUMNS = [
  'worklog_id','issue_key','issue_summary','issue_type','priority','status',
  'assignee_email','epic_key','sprint','author_email','author_account_id',
  'time_spent_seconds','started','comment','issue_description'
];

const baseSelect = `
SELECT
  worklogs.id as worklog_id,
  issues.key as issue_key,
  issues.summary as issue_summary,
  issues.issue_type,
  issues.priority,
  issues.status,
  issues.assignee_email,
  issues.epic_key,
  issues.sprint,
  issues.description as issue_description,
  worklogs.author_email,
  worklogs.author_account_id,
  worklogs.time_spent_seconds,
  worklogs.started,
  worklogs.comment
FROM worklogs
JOIN issues ON issues.id = worklogs.issue_id
`;

async function writeCsvToFile(rows, filePath) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  const stringifier = stringify({ header: true, columns: CSV_COLUMNS });
  const out = fs.createWriteStream(filePath);
  const passThrough = new (await import('stream')).PassThrough();
  const p = pipe(passThrough, out);
  stringifier.on('readable', () => {
    let row;
    while ((row = stringifier.read()) !== null) passThrough.write(row);
  });
  stringifier.on('end', () => passThrough.end());
  for (const r of rows) stringifier.write(r);
  stringifier.end();
  await p;
}

async function ensureCsvForRange({ from, to, group, strict = true }) {
  const external = csvExternalPathFor({ from, to, group });
  if (await fileExists(external)) return external; // priorytet dla importu z serwletu

  const suffix = strict ? '' : '__loose';
  const fp = path.join(CACHE_DIR, `${slug(group)}__${from}__${to}${suffix}.csv`);
  if (await fileExists(fp)) return fp;

  // Sync on-demand → DB → CSV
  await syncWorklogsForRangeAndGroup({ from, to, groupLabel: group }); // ← naprawione

  const { whereSql, params } = await buildWhere({ from, to, sprint: '', group, requireBoth: true }); // ← bez groupMode
  const rows = db.prepare(`${baseSelect} ${whereSql} ORDER BY worklogs.started DESC`).all(params);
  await writeCsvToFile(rows, fp);
  return fp;
}


async function readCsvAsJsonPreview(fp, limit = 100) {
  const text = await fs.promises.readFile(fp, 'utf8');
  const lines = text.split(/\r?\n/);
  if (!lines.length) return { count: 0, hours_total: 0, rows: [] };

  const parseLine = (s) => {
    const out = [];
    let cur = '';
    let inQ = false;
    for (let i = 0; i < s.length; i++) {
      const c = s[i];
      if (inQ) {
        if (c === '"' && s[i + 1] === '"') { cur += '"'; i++; }
        else if (c === '"') { inQ = false; }
        else { cur += c; }
      } else {
        if (c === '"') inQ = true;
        else if (c === ',') { out.push(cur); cur = ''; }
        else { cur += c; }
      }
    }
    out.push(cur);
    return out;
  };

  


  const headerLine = lines.find(l => l.trim().length > 0);
  if (!headerLine) return { count: 0, hours_total: 0, rows: [] };
  const header = parseLine(headerLine);

  const rows = [];
  for (let i = 1; i < lines.length && rows.length < limit; i++) {
    const raw = lines[i];
    if (!raw || raw === '') continue;
    const cols = parseLine(raw);
    if (cols.length === 1 && cols[0] === '') continue;
    const obj = {};
    for (let h = 0; h < header.length; h++) obj[header[h]] = cols[h] ?? '';
    rows.push(obj);
  }

  const hours_total = rows.reduce((acc, r) => {
    const sec = Number(r.time_spent_seconds || 0);
    return acc + (Number.isFinite(sec) ? sec : 0);
  }, 0) / 3600;

  return { count: rows.length, hours_total, rows };
}
function splitCsvLines(text) {
  // bardzo prosty parser linii CSV (zgodny z tym, którego już używamy)
  const lines = [];
  let row = [], field = '', inQ = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQ) {
      if (c === '"' && text[i+1] === '"') { field += '"'; i++; }
      else if (c === '"') { inQ = false; }
      else { field += c; }
    } else {
      if (c === '"') inQ = true;
      else if (c === ',') { row.push(field); field = ''; }
      else if (c === '\n' || c === '\r') {
        if (field.length || row.length){ row.push(field); lines.push(row); row=[]; field=''; }
        if (c === '\r' && text[i+1] === '\n') i++;
      } else field += c;
    }
  }
  if (field.length || row.length) { row.push(field); lines.push(row); }
  return lines;
}

function csvToObjects(text){
  const rows = splitCsvLines(text);
  if (!rows.length) return [];
  const header = rows[0].map(h => String(h||'').trim());
  const idx = new Map(header.map((h,i)=>[h.toLowerCase(), i]));
  function get(r, names){
    for (const n of names) {
      const i = idx.get(String(n).toLowerCase());
      if (i != null) return r[i] ?? '';
    }
    return '';
  }
  return rows.slice(1).filter(r=>r.length>1).map(r=>{
    const obj = {};
    for (let i=0;i<header.length;i++) obj[header[i]] = r[i] ?? '';
    obj._get = (names)=>get(r, Array.isArray(names)?names:[names]);
    return obj;
  });
}

function toIsoDateTime(v){
  if (v == null) return '';
  const s = String(v).trim();
  if (!s) return '';
  // czyste cyfry → spróbuj epoch w ms/sek
  if (/^\d{10,}$/.test(s)) {
    const ms = s.length > 10 ? parseInt(s,10) : parseInt(s,10)*1000;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? '' : d.toISOString();
  }
  const d = new Date(s);
  return isNaN(d.getTime()) ? '' : d.toISOString();
}

function safeHoursToSeconds(h){
  const n = parseFloat(String(h).replace(',', '.'));
  return Number.isFinite(n) ? Math.round(n*3600) : 0;
}

function makeWorklogId(issueKey, startedIso, user){
  const base = `${issueKey}::${startedIso}::${user}`;
  return crypto.createHash('sha1').update(base).digest('hex');
}

/** Mapuj CSV z serwletu do naszych kolumn */
function mapServletCsvToOurRows(text){
  const objs = csvToObjects(text);
  const out = [];
  for (const o of objs){
    const g = o._get;

    const issue_key   = g(['Issue key','Issue Key','Key']) || '';
    const issue_type  = g(['Issue type','Type']) || '';
    const priority    = g(['Issue priority','Priority']) || '';
    const issue_summary = g(['Issue summary','Summary']) || '';
    const comment     = g(['Comment']) || '';
    const project     = g(['Project']) || '';
    const sprint      = g(['Sprint']) || '';
    const status      = g(['Status']) || '';
    const assignee    = g(['Assignee']) || '';
    const authorRaw   = g(['User']) || ''; // zazwyczaj displayName
    const timeHours   = g(['Time spent (hours)','Time spent']) || '0';
    const startedIso  = toIsoDateTime(g(['Work start time','Work Start Time','Start time','Timestamp','Time']));

    const author_email = /@/.test(authorRaw) ? authorRaw : '';
    const author_account_id = author_email ? '' : authorRaw; // frontend wyświetli to ładnie

    const row = {
      worklog_id: makeWorklogId(issue_key, startedIso || '', authorRaw),
      issue_key,
      issue_summary,
      issue_type,
      priority,
      status,
      assignee_email: /@/.test(assignee) ? assignee : '',
      epic_key: '',
      sprint: sprint || project || '',
      author_email,
      author_account_id,
      time_spent_seconds: safeHoursToSeconds(timeHours),
      started: startedIso || '',
      comment,
      issue_description: ''
    };
    // puste issue_key → pomiń
    if (row.issue_key) out.push(row);
  }
  return out;
}
/* ================= dynamic WHERE (safe on group) ================= */
async function buildWhere({ from, to, sprint, group, requireBoth = true }) {
  if (requireBoth && (!from || !to || !group)) {
    return { whereSql: 'WHERE 1=0', params: {}, membersMeta: { ok: false, reason: 'missing-filters' } };
  }
  const where = [];
  const params = {};
  if (from) { where.push('substr(worklogs.started,1,10) >= @from'); params.from = from; }
  if (to)   { where.push('substr(worklogs.started,1,10) <= @to');   params.to   = to; }
  if (sprint) {
    where.push('issues.sprint LIKE @sprint');
    params.sprint = `%${sprint}%`;
  }

  let membersMeta = { ok: true, reason: 'api-or-override' };

  if (group) {
    const g = GROUPS.find(x =>
      x.label.toLowerCase() === group.toLowerCase() ||
      x.name.toLowerCase()  === group.toLowerCase()
    );
    if (!g) {
      // łagodnie: bez filtra autorów (lepszy nadmiar niż niedomiar)
      membersMeta = { ok: false, reason: 'group-not-configured' };
      return { whereSql: where.length ? ('WHERE ' + where.join(' AND ')) : '', params, membersMeta };
    }

    const members = await getGroupMembersMerged(g.label, g.name);
    const ids    = members.map(m => (m.accountId || '').toLowerCase()).filter(Boolean);
    const emails = members.map(m => (m.emailAddress || '').toLowerCase()).filter(Boolean);

    const parts = [];
    if (ids.length) {
      const ph = ids.map((_, i) => `@gid${i}`);
      ids.forEach((v, i) => (params[`gid${i}`] = v));
      parts.push(`LOWER(worklogs.author_account_id) IN (${ph.join(',')})`);
    }
    if (emails.length) {
      const ph = emails.map((_, i) => `@gem${i}`);
      emails.forEach((v, i) => (params[`gem${i}`] = v));
      parts.push(`LOWER(worklogs.author_email) IN (${ph.join(',')})`);
    }

    if (parts.length) {
      where.push(`(${parts.join(' OR ')})`);
    } else {
      // fallback: brak rozpoznanych członków – nie zaciskamy
      membersMeta = { ok: false, reason: 'no-members' };
    }
  } else if (requireBoth) {
    membersMeta = { ok: false, reason: 'missing-group' };
  }

  return { whereSql: where.length ? ('WHERE ' + where.join(' AND ')) : '', params, membersMeta };
}

/* ================= express ================= */
const app = express();
app.use(express.static('public'));

// pozwól wysłać czysty CSV jako body (max ~50 MB)
app.use(express.text({ type: ['text/csv','text/plain','*/*'], limit: '50mb' }));

/* debug: członkowie grupy */
app.get('/api/debug/members', async (req, res) => {
  const groupQ = String(req.query.group || '').trim();
  if (!groupQ) return res.status(400).json({ error: 'Missing ?group=' });
  const g = GROUPS.find(x =>
    x.label.toLowerCase() === groupQ.toLowerCase() ||
    x.name.toLowerCase()  === groupQ.toLowerCase()
  );
  if (!g) return res.status(404).json({ error: 'Group not configured in JIRA_GROUPS' });
  const members = await getGroupMembersMerged(g.label, g.name);
  res.json({ group: g, count: members.length, members });
});

app.get('/redirect', (req, res) => {
  const key = String(req.query.key || '').trim();
  if (!key) return res.status(400).send('Missing ?key=');
  const target = `${JIRA_BASE_URL.replace(/\/+$/, '')}/browse/${encodeURIComponent(key)}`;
  res.redirect(302, target);
});

app.get('/api/groups', (_req, res) => res.json(GROUPS));

/* refresh-cache → hybrydowy pełny sync + CSV */
app.post('/api/refresh-cache', async (req, res) => {
  const from  = (req.query.from  || '').trim();
  const to    = (req.query.to    || '').trim();
  const group = (req.query.group || '').trim();
  if (!from || !to || !group) return res.status(400).json({ error: 'Missing from/to/group' });

  try {
    const fp = csvPathFor({ from, to, group });

    // diagnostyka: ilu członków wykryto
    const g = GROUPS.find(x =>
      x.label.toLowerCase() === group.toLowerCase() ||
      x.name.toLowerCase()  === group.toLowerCase()
    );
    let membersCount = 0;
    if (g) {
      const merged = await getGroupMembersMerged(g.label, g.name).catch(()=>[]);
      membersCount = Array.isArray(merged) ? merged.length : 0;
    }

    const syncStats = await syncWorklogsForRangeAndGroup({ from, to, groupLabel: group });

    // CSV z DB po twardym cięciu dat/grupy
    const { whereSql, params } = await buildWhere({ from, to, sprint: '', group, requireBoth: true });
    const rows = db.prepare(`${baseSelect} ${whereSql} ORDER BY worklogs.started DESC`).all(params);
    await writeCsvToFile(rows, fp);

    res.setHeader('Cache-Control', 'no-store, max-age=0');
    res.status(200).json({
      ok: true,
      refreshed: path.basename(fp),
      issuesFetched: syncStats.issues,
      worklogsInserted: syncStats.worklogs,
      retriedWithoutMembers: syncStats.retried,
      csvRows: rows.length,
      groupMembersDetected: membersCount,
      modeUsed: syncStats.mode || (syncStats.retried ? 'search-fallback' : 'worklog-first')
    });
  } catch (e) {
    res.status(500).json({ error: 'refresh-failed', message: e?.message || String(e) });
  }
});

function csvExternalPathFor(q) {
  // osobny sufiks, żeby nie nadpisywać naszych plików
  return path.join(CACHE_DIR, `${slug(q.group)}__${q.from}__${q.to}__external.csv`);
}
/* JSON (CSV-first, auto-create if missing) */
const DEFAULT_PREVIEW_LIMIT = 5000;
app.get('/api/report', async (req, res) => {
  const from   = (req.query.from   || '').trim();
  const to     = (req.query.to     || '').trim();
  const group  = (req.query.group  || '').trim();
  const limitQ = Number(req.query.limit || DEFAULT_PREVIEW_LIMIT);
  if (!from || !to || !group) return res.status(204).send();

  try {
    let fp = csvPathFor({ from, to, group });
    if (!(await fileExists(fp))) {
      // UŻYJ ścieżki zwróconej przez ensureCsvForRange (może to być ...__external.csv)
      fp = await ensureCsvForRange({ from, to, group });
    }
    const preview = await readCsvAsJsonPreview(fp, limitQ);
    res.setHeader('Cache-Control', 'no-store, max-age=0');
    res.json(preview);
  } catch (e) {
    res.status(500).json({ error: 'csv-read-failed', message: e?.message || String(e) });
  }
});


/* CSV stream (auto-create) */
app.get('/api/report.csv', async (req, res) => {
  const from   = (req.query.from   || '').trim();
  const to     = (req.query.to     || '').trim();
  const group  = (req.query.group  || '').trim();

  if (!from || !to || !group) return res.status(204).send();

  try {
    const fp = await ensureCsvForRange({ from, to, group });
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${path.basename(fp)}"`);
    res.setHeader('Cache-Control', 'no-store, max-age=0');
    const stream = fs.createReadStream(fp);
    stream.pipe(res);
  } catch (e) {
    res.status(500).json({ error: 'csv-stream-failed', message: e?.message || String(e) });
  }
});

/* insights (na DB) */
app.get('/api/insights', async (req, res) => {
  const from   = (req.query.from   || '').trim();
  const to     = (req.query.to     || '').trim();
  const group  = (req.query.group  || '').trim();
  if (!from || !to || !group) return res.status(204).send();

  try {
    const { whereSql, params } = await buildWhere({ from, to, sprint: '', group, requireBoth: true });
    const hours_total =
      db.prepare(`SELECT COALESCE(SUM(time_spent_seconds),0)/3600.0 AS h FROM worklogs JOIN issues ON issues.id = worklogs.issue_id ${whereSql}`).get(params).h || 0;
    res.setHeader('Cache-Control', 'no-store, max-age=0');
    res.json({ hours_total });
  } catch (e) {
    res.status(500).json({ error: 'insights-failed', message: e?.message || String(e) });
  }
});



/* health */
app.get('/health', (_req, res) => res.json({ ok: true, fast_mode: FAST }));

/* optional background sync */
async function syncOnce({ jql = JQL }) {
  if (!jql) return;
  const fields = [
    'summary','issuetype','priority','status','assignee','parent',
    'customfield_10008','customfield_10014','updated','description'
  ];
  const issues = await pagedSearch(jql, fields).catch(() => []);

  const trx = db.transaction(() => {
    for (const it of issues) {
      const f = it.fields || {};
      const sprintRaw = f['customfield_10008'];
      const sprintName = Array.isArray(sprintRaw) && sprintRaw.length ? (sprintRaw.at(-1)?.name || sprintRaw.at(-1)) : null;
      const epicKey = f['customfield_10014'] || (f.parent && f.parent.key?.startsWith('EPIC') ? f.parent.key : null);
      const descPlain = typeof f.description === 'string' ? f.description
                        : (f.description && typeof f.description === 'object' ? adfToPlain(f.description) : null);

      upsertIssueStmt.run({
        id: it.id,
        key: it.key,
        summary: f.summary || null,
        issue_type: f.issuetype?.name || null,
        priority: f.priority?.name || null,
        status: f.status?.name || null,
        assignee_email: f.assignee?.emailAddress || null,
        epic_key: epicKey || null,
        sprint: sprintName || null,
        updated_at: f.updated || null,
        description: descPlain || null
      });
    }
  });
  trx();

  // worklogi robi główny flow hybrydowy
}

if (!FAST) {
  syncOnce({}).catch(e => console.error('Initial sync error:', e.message));
  cron.schedule(CRON_SCHEDULE, () => {
    syncOnce({}).catch(e => console.error('Scheduled sync error:', e.message));
  });
}

// === [IMPORT CSV z serwleta] ================================================



// POST /api/import-csv?from=YYYY-MM-DD&to=YYYY-MM-DD&group=Label
// Body: surowy CSV z serwleta → mapujemy do naszego modelu (CSV_COLUMNS) → zapisujemy jako ...__external.csv
app.post('/api/import-csv', async (req, res) => {
  try {
    const from  = String(req.query.from  || '').trim();
    const to    = String(req.query.to    || '').trim();
    const group = String(req.query.group || '').trim();
    const body  = typeof req.body === 'string' ? req.body : '';

    if (!from || !to || !group) {
      return res.status(400).json({ error: 'missing-params', message: 'from, to, group required' });
    }
    if (!body.trim()) {
      return res.status(400).json({ error: 'empty-body', message: 'CSV body is empty' });
    }

    // Używamy JEDYNEJ, globalnej wersji mapera – zgodnej z CSV_COLUMNS
    const rows = mapServletCsvToOurRows(body);

    // zapisujemy w tej samej przestrzeni co reszta, ale z sufiksem __external.csv
    const fp = csvExternalPathFor({ from, to, group });
    await writeCsvToFile(rows, fp);

    return res.json({ ok: true, mappedRows: rows.length, saved: path.basename(fp) });
  } catch (e) {
    console.error('import-csv failed:', e);
    return res.status(500).json({ error: 'import-failed', message: e?.message || 'Internal error' });
  }
});


app.listen(PORT, () => {
  console.log(`🚀 Server listening on http://localhost:${PORT}`);
  console.log(`   FAST_MODE: ${FAST ? 'ON' : 'OFF'}`);
  console.log(`   CSV dir: ${CACHE_DIR}`);
});
