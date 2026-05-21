import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import sql from 'mssql';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 8081;
const DATA_DIR = path.join(__dirname, '.data');
const CONFIG_FILE = path.join(DATA_DIR, 'config.json');
const REPORTS_FILE = path.join(DATA_DIR, 'reports.json');
const POI_CONFIG_FILE = path.join(DATA_DIR, 'poi-config.json');
const POI_RUNS_FILE = path.join(DATA_DIR, 'poi-runs.json');
const POI_RUN_STATE_FILE = path.join(DATA_DIR, 'poi-run-state.json');
const EXECUTION_SETS_FILE = path.join(DATA_DIR, 'execution-sets.json');
const POI_CLUSTER_DIR = path.join(DATA_DIR, 'poi-clusters');
const SCHEDULE_LOG_FILE = path.join(DATA_DIR, 'schedule-log.json');
const SCHEDULE_STATE_FILE = path.join(DATA_DIR, 'schedule-state.json');
const SQL_EXPORT_COLUMNS = ['key', 'summary', 'status', 'assignee', 'updated', 'priority', 'issuetype'];
const POI_INFRACTION_TYPES = [
  { id: 0, label: 'N/D' },
  { id: 1, label: 'Traffic fines' },
  { id: 2, label: 'Speeding' },
  { id: 3, label: 'Parking' },
  { id: 4, label: 'Tolling' },
  { id: 5, label: 'Safety' },
  { id: 6, label: 'Other (absence of insurance, certificates, etc)' }
];
const SCHEDULE_LOG_LIMIT = 20;
const POI_RUN_LOG_LIMIT = 30;
const EARTH_RADIUS_KM = 6371;

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(POI_CLUSTER_DIR, { recursive: true });
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

function loadJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function saveJson(file, value) {
  fs.writeFileSync(file, JSON.stringify(value, null, 2));
}
function getConfig() { return loadJson(CONFIG_FILE, null); }
function setConfig(cfg) { saveJson(CONFIG_FILE, cfg); }
function getReports() { return loadJson(REPORTS_FILE, []); }
function setReports(reports) { saveJson(REPORTS_FILE, reports); }
function getPoiConfig() { return loadJson(POI_CONFIG_FILE, defaultPoiConfig()); }
function setPoiConfig(cfg) { saveJson(POI_CONFIG_FILE, cfg); }
function getPoiRuns() { return loadJson(POI_RUNS_FILE, []); }
function setPoiRuns(runs) { saveJson(POI_RUNS_FILE, runs); }
function defaultPoiRunState() {
  return {
    running: false,
    status: 'idle',
    progress: 0,
    message: 'Nessuna esecuzione avviata',
    stage: 'idle',
    runId: null,
    setId: null,
    setName: '',
    startedAt: null,
    finishedAt: null,
    elapsedMs: null,
    elapsedLabel: null,
    durationMs: null,
    durationLabel: null,
    summary: null,
    details: null,
    error: null,
    updatedAt: null
  };
}
let poiRunStateCache = loadJson(POI_RUN_STATE_FILE, defaultPoiRunState());
let currentPoiRunControl = null;

function createPoiRunCancellationError(reason = 'Esecuzione interrotta dall\'utente') {
  const error = new Error(reason);
  error.code = 'POI_RUN_CANCELLED';
  error.status = 409;
  return error;
}

function isPoiRunCancellationError(error) {
  return Boolean(error && (error.code === 'POI_RUN_CANCELLED' || error.code === 'ERR_POI_RUN_CANCELLED'));
}

function createPoiRunControl() {
  let request = null;
  const cancelListeners = new Set();
  return {
    cancelled: false,
    cancelReason: 'Esecuzione interrotta dall\'utente',
    onCancel(listener) {
      if (typeof listener !== 'function') return () => {};
      if (this.cancelled) {
        try {
          listener(this.cancelReason);
        } catch (error) {
          console.error('Failed to run POI cancel listener', error);
        }
        return () => {};
      }
      cancelListeners.add(listener);
      return () => cancelListeners.delete(listener);
    },
    bindRequest(nextRequest) {
      request = nextRequest || null;
      if (this.cancelled && request && typeof request.cancel === 'function') {
        try {
          request.cancel();
        } catch (error) {
          console.error('Failed to cancel POI request after abort', error);
        }
      }
    },
    abort(reason = 'Esecuzione interrotta dall\'utente') {
      if (this.cancelled) return;
      this.cancelled = true;
      this.cancelReason = reason;
      for (const listener of cancelListeners) {
        try {
          listener(reason);
        } catch (error) {
          console.error('Failed to run POI cancel listener', error);
        }
      }
      cancelListeners.clear();
      if (request && typeof request.cancel === 'function') {
        try {
          request.cancel();
        } catch (error) {
          console.error('Failed to cancel POI request', error);
        }
      }
    },
    throwIfCancelled() {
      if (this.cancelled) {
        throw createPoiRunCancellationError(this.cancelReason);
      }
    }
  };
}
function formatDurationMs(ms) {
  const totalSeconds = Math.max(0, Math.floor(Number(ms) / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}
function getRunTiming(startedAt, finishedAt = null) {
  const startMs = new Date(startedAt).getTime();
  const endMs = new Date(finishedAt || new Date()).getTime();
  const durationMs = Number.isFinite(startMs) && Number.isFinite(endMs) ? Math.max(0, endMs - startMs) : 0;
  return {
    durationMs,
    durationLabel: formatDurationMs(durationMs)
  };
}
function getPoiRunState() {
  const state = {
    ...defaultPoiRunState(),
    ...(poiRunStateCache && typeof poiRunStateCache === 'object' ? poiRunStateCache : {})
  };
  if (state.startedAt) {
    const timing = getRunTiming(state.startedAt, state.finishedAt || (state.running ? new Date() : state.finishedAt));
    state.elapsedMs = timing.durationMs;
    state.elapsedLabel = timing.durationLabel;
    if (!state.running && state.finishedAt) {
      state.durationMs = timing.durationMs;
      state.durationLabel = timing.durationLabel;
    }
  }
  return {
    ...state
  };
}
function setPoiRunState(state) {
  poiRunStateCache = {
    ...defaultPoiRunState(),
    ...(state && typeof state === 'object' ? state : {})
  };
  saveJson(POI_RUN_STATE_FILE, poiRunStateCache);
}
function getExecutionSets() { return loadJson(EXECUTION_SETS_FILE, []); }
function setExecutionSets(sets) { saveJson(EXECUTION_SETS_FILE, sets); }
function getScheduleLog() { return loadJson(SCHEDULE_LOG_FILE, { entries: [] }); }
function setScheduleLog(log) { saveJson(SCHEDULE_LOG_FILE, log); }
function getScheduleState() { return loadJson(SCHEDULE_STATE_FILE, { running: false, lastAutoRunKey: null }); }
function setScheduleState(state) { saveJson(SCHEDULE_STATE_FILE, state); }
function normalizeInfractionIds(values) {
  const allowedInfractionIds = new Set(POI_INFRACTION_TYPES.map(item => item.id));
  const selectedIds = Array.isArray(values)
    ? values
      .map(item => Number.parseInt(String(item), 10))
      .filter(item => Number.isInteger(item) && allowedInfractionIds.has(item))
    : [];
  return Array.from(new Set(selectedIds));
}
function defaultPoiConfig() {
  return {
    sqlConnectionString: '',
    tableName: '',
    dbscan: {
      eps: 0.5,
      minPoints: 5
    },
    filters: {
      citta: '',
      idtipoinfrazione: []
    }
  };
}
function normalizePoiConfig(value = {}) {
  const fallback = defaultPoiConfig();
  const raw = value && typeof value === 'object' ? value : {};
  const dbscan = raw.dbscan && typeof raw.dbscan === 'object' ? raw.dbscan : {};
  const filters = raw.filters && typeof raw.filters === 'object' ? raw.filters : {};

  return {
    sqlConnectionString: String(raw.sqlConnectionString || '').trim(),
    tableName: String(raw.tableName || '').trim(),
    dbscan: {
      eps: Number.isFinite(Number(dbscan.eps)) && Number(dbscan.eps) > 0 ? Number(dbscan.eps) : fallback.dbscan.eps,
      minPoints: Number.isFinite(Number(dbscan.minPoints)) && Number(dbscan.minPoints) > 0
        ? Math.max(1, Math.floor(Number(dbscan.minPoints)))
        : fallback.dbscan.minPoints
    },
    filters: {
      citta: String(filters.citta || '').trim(),
      idtipoinfrazione: normalizeInfractionIds(filters.idtipoinfrazione)
    }
  };
}
function normalizeExecutionSet(value = {}, existing = null) {
  const raw = value && typeof value === 'object' ? value : {};
  const dbscan = raw.dbscan && typeof raw.dbscan === 'object' ? raw.dbscan : {};
  const filters = raw.filters && typeof raw.filters === 'object' ? raw.filters : {};
  const name = String(raw.name || '').trim();
  if (!name) throw new Error('Missing execution set name');

  return {
    id: existing?.id || String(raw.id || crypto.randomUUID()),
    name,
    dbscan: {
      eps: Number.isFinite(Number(dbscan.eps)) && Number(dbscan.eps) > 0 ? Number(dbscan.eps) : 0.5,
      minPoints: Number.isFinite(Number(dbscan.minPoints)) && Number(dbscan.minPoints) > 0
        ? Math.max(1, Math.floor(Number(dbscan.minPoints)))
        : 5
    },
    filters: {
      citta: String(filters.citta || '').trim(),
      idtipoinfrazione: normalizeInfractionIds(filters.idtipoinfrazione)
    },
    createdAt: existing?.createdAt || raw.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
}
function appendPoiRun(entry) {
  const runs = Array.isArray(getPoiRuns()) ? getPoiRuns() : [];
  runs.unshift(entry);
  setPoiRuns(runs.slice(0, POI_RUN_LOG_LIMIT));
}
function enrichClusterFileRecord(file = {}) {
  const nextFile = {
    ...file
  };
  const compactness = Number(nextFile.compactnessPercent);
  const meanDistanceKm = Number(nextFile.meanDistanceKm);
  const referenceRadiusKm = Number(nextFile.referenceRadiusKm);
  if (Number.isFinite(compactness) && Number.isFinite(meanDistanceKm) && Number.isFinite(referenceRadiusKm)) {
    return { file: nextFile, changed: false };
  }

  const absolutePath = nextFile.path ? path.resolve(__dirname, String(nextFile.path)) : null;
  const allowedPrefix = `${path.resolve(POI_CLUSTER_DIR)}${path.sep}`;
  if (!absolutePath || !absolutePath.startsWith(allowedPrefix) || !fs.existsSync(absolutePath)) {
    return { file: nextFile, changed: false };
  }

  const cluster = loadJson(absolutePath, null);
  if (!cluster || typeof cluster !== 'object') {
    return { file: nextFile, changed: false };
  }

  const metrics = calculateClusterCompactness(Array.isArray(cluster.Punti) ? cluster.Punti : []);
  nextFile.compactnessPercent = Number.isFinite(compactness) ? compactness : metrics.compactnessPercent;
  nextFile.meanDistanceKm = Number.isFinite(meanDistanceKm) ? meanDistanceKm : metrics.meanDistanceKm;
  nextFile.referenceRadiusKm = Number.isFinite(referenceRadiusKm) ? referenceRadiusKm : metrics.referenceRadiusKm;
  return { file: nextFile, changed: true, cluster, metrics, absolutePath };
}
function backfillPoiRunArtifacts() {
  const runs = getPoiRuns();
  if (!Array.isArray(runs) || !runs.length) return false;

  let changed = false;
  const nextRuns = runs.map(run => {
    if (!run || typeof run !== 'object') return run;
    const artifacts = run.artifacts && typeof run.artifacts === 'object' ? run.artifacts : null;
    if (!artifacts || !Array.isArray(artifacts.files) || !artifacts.files.length) return run;

    let runChanged = false;
    const nextFiles = artifacts.files.map(file => {
      const result = enrichClusterFileRecord(file);
      if (!result.changed) return file;
      runChanged = true;
      if (result.cluster && result.absolutePath) {
        const nextCluster = {
          ...result.cluster,
          CompattezzaPercentuale: result.cluster.CompattezzaPercentuale ?? result.metrics.compactnessPercent,
          DistanzaMediaKm: result.cluster.DistanzaMediaKm ?? result.metrics.meanDistanceKm,
          RaggioRiferimentoKm: result.cluster.RaggioRiferimentoKm ?? result.metrics.referenceRadiusKm
        };
        saveJson(result.absolutePath, nextCluster);
      }
      return result.file;
    });

    if (!runChanged) return run;
    changed = true;
    return {
      ...run,
      artifacts: {
        ...artifacts,
        files: nextFiles
      }
    };
  });

  if (changed) setPoiRuns(nextRuns);
  return changed;
}
function summarizePoiConfig(cfg = {}) {
  return {
    tableName: String(cfg?.tableName || ''),
    dbscan: {
      eps: cfg?.dbscan?.eps ?? null,
      minPoints: cfg?.dbscan?.minPoints ?? null
    },
    filters: {
      citta: String(cfg?.filters?.citta || ''),
      idtipoinfrazione: Array.isArray(cfg?.filters?.idtipoinfrazione) ? cfg.filters.idtipoinfrazione : []
    }
  };
}
backfillPoiRunArtifacts();
function updatePoiRunState(patch = {}) {
  const next = {
    ...defaultPoiRunState(),
    ...getPoiRunState(),
    ...patch,
    updatedAt: patch.updatedAt || new Date().toISOString()
  };
  setPoiRunState(next);
  return next;
}
const bootPoiRunState = getPoiRunState();
if (bootPoiRunState.running) {
  const finishedAt = new Date().toISOString();
  const timing = getRunTiming(bootPoiRunState.startedAt, finishedAt);
  setPoiRunState({
    ...bootPoiRunState,
    running: false,
    status: 'failed',
    progress: 100,
    message: 'Esecuzione interrotta dal riavvio del servizio',
    finishedAt,
    elapsedMs: timing.durationMs,
    elapsedLabel: timing.durationLabel,
    durationMs: timing.durationMs,
    durationLabel: timing.durationLabel,
    error: bootPoiRunState.error || { message: 'Execution interrupted by service restart' }
  });
}
function formatPoiDate(value) {
  if (!value) return '';
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toISOString().slice(0, 19);
}
function sanitizePathSegment(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'run';
}
function haversineKm(a, b) {
  const toRad = degrees => degrees * (Math.PI / 180);
  const dLat = toRad(b.Latitudine - a.Latitudine);
  const dLon = toRad(b.Longitudine - a.Longitudine);
  const lat1 = toRad(a.Latitudine);
  const lat2 = toRad(b.Latitudine);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = (sinLat ** 2) + Math.cos(lat1) * Math.cos(lat2) * (sinLon ** 2);
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.min(1, Math.sqrt(h)));
}
function normalizePoiPoint(row = {}) {
  return {
    Id: row.Id == null ? null : Number(row.Id),
    IdDataentry: row.IdDataentry == null ? null : Number(row.IdDataentry),
    IdOcr: row.IdOcr == null ? null : Number(row.IdOcr),
    Latitudine: Number(row.Latitudine),
    Longitudine: Number(row.Longitudine),
    Citta: String(row.Citta || '').trim(),
    IndirizzoFormattato: String(row.IndirizzoFormattato || ''),
    DataInfrazione: formatPoiDate(row.DataInfrazione),
    IdTipoInfrazione: row.IdTipoInfrazione == null ? null : Number(row.IdTipoInfrazione)
  };
}
function percentile(values = [], fraction = 0.5) {
  if (!Array.isArray(values) || !values.length) return 0;
  const sorted = values
    .map(value => Number(value))
    .filter(value => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (!sorted.length) return 0;
  if (sorted.length === 1) return sorted[0];
  const clamped = Math.max(0, Math.min(1, Number(fraction) || 0));
  const index = (sorted.length - 1) * clamped;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  return sorted[lower] + ((index - lower) * (sorted[upper] - sorted[lower]));
}
function calculateClusterCompactness(clusterPoints = []) {
  const points = Array.isArray(clusterPoints) ? clusterPoints.filter(point => Number.isFinite(point?.Latitudine) && Number.isFinite(point?.Longitudine)) : [];
  if (!points.length) {
    return {
      centroidLatitudine: null,
      centroidLongitudine: null,
      meanDistanceKm: 0,
      referenceRadiusKm: 0,
      compactnessPercent: 0
    };
  }

  const centroidLatitudine = points.reduce((sum, point) => sum + point.Latitudine, 0) / points.length;
  const centroidLongitudine = points.reduce((sum, point) => sum + point.Longitudine, 0) / points.length;
  const centroid = { Latitudine: centroidLatitudine, Longitudine: centroidLongitudine };
  const distances = points.map(point => haversineKm(centroid, point));
  const meanDistanceKm = distances.reduce((sum, value) => sum + value, 0) / distances.length;
  const referenceRadiusKm = Math.max(percentile(distances, 0.95), meanDistanceKm, 0.001);
  const compactnessPercent = Math.max(0, Math.min(100, Math.round((1 - (meanDistanceKm / referenceRadiusKm)) * 100)));

  return {
    centroidLatitudine,
    centroidLongitudine,
    meanDistanceKm,
    referenceRadiusKm,
    compactnessPercent
  };
}
function buildPoiSourceWhereClauses(filters = {}) {
  const where = [
    'TRY_CONVERT(float, [Latitudine]) IS NOT NULL',
    'TRY_CONVERT(float, [Longitudine]) IS NOT NULL'
  ];
  if (filters.citta) {
    where.push('LTRIM(RTRIM(CONVERT(nvarchar(255), [Citta]))) = @citta');
  }
  if (Array.isArray(filters.idtipoinfrazione) && filters.idtipoinfrazione.length) {
    where.push(`TRY_CONVERT(int, [IdTipoInfrazione]) IN (${filters.idtipoinfrazione.map((_, index) => `@infraction_${index}`).join(', ')})`);
  }
  return where;
}

function buildPoiSourceQuery(targetTable, filters = {}) {
  const where = buildPoiSourceWhereClauses(filters);
  return `
    SELECT
      COUNT_BIG(1) OVER() AS [TotalRows],
      [Id],
      [IdDataentry],
      [IdOcr],
      TRY_CONVERT(float, [Latitudine]) AS [Latitudine],
      TRY_CONVERT(float, [Longitudine]) AS [Longitudine],
      CONVERT(nvarchar(255), [Citta]) AS [Citta],
      CONVERT(nvarchar(max), [IndirizzoFormattato]) AS [IndirizzoFormattato],
      [DataInfrazione],
      TRY_CONVERT(int, [IdTipoInfrazione]) AS [IdTipoInfrazione]
    FROM ${targetTable}
    WHERE ${where.join('\n      AND ')}
  `;
}
function bindPoiSourceFilters(request, filters = {}) {
  if (filters.citta) {
    request.input('citta', sql.NVarChar(255), String(filters.citta).trim());
  }
  const infractionIds = normalizeInfractionIds(filters.idtipoinfrazione);
  infractionIds.forEach((id, index) => {
    request.input(`infraction_${index}`, sql.Int, id);
  });
  return {
    citta: String(filters.citta || '').trim(),
    idtipoinfrazione: infractionIds
  };
}
async function loadPoiSourcePoints(config, executionSet, onProgress = () => {}, control = null) {
  const targetTable = normalizeReportTableName(config?.tableName);
  const connectionConfig = parseSqlConnectionConfig(config?.sqlConnectionString);
  const pool = await new sql.ConnectionPool(connectionConfig).connect();
  try {
    control?.throwIfCancelled();
    const filters = executionSet?.filters || {};
    const normalizedFilters = {
      citta: String(filters.citta || '').trim(),
      idtipoinfrazione: normalizeInfractionIds(filters.idtipoinfrazione)
    };
    const rows = [];
    let totalRows = 0;
    const streamRequest = pool.request();
    bindPoiSourceFilters(streamRequest, normalizedFilters);
    streamRequest.stream = true;

    await new Promise((resolve, reject) => {
      let settled = false;
      let cancelSubscription = null;
      let rowsRead = 0;
      let lastEmittedRows = 0;
      let lastEmittedAt = 0;
      const settleReject = (reason = control?.cancelReason) => {
        if (settled) return;
        settled = true;
        if (cancelSubscription) {
          cancelSubscription();
          cancelSubscription = null;
        }
        reject(createPoiRunCancellationError(reason));
      };
      const settleResolve = () => {
        if (settled) return;
        settled = true;
        if (cancelSubscription) {
          cancelSubscription();
          cancelSubscription = null;
        }
        resolve();
      };

      cancelSubscription = control?.onCancel?.((reason) => {
        try {
          if (typeof streamRequest.cancel === 'function') {
            streamRequest.cancel();
          }
        } catch (error) {
          console.error('Failed to cancel POI stream request from control listener', error);
        }
        settleReject(reason);
      }) || null;

      streamRequest.on('row', row => {
        if (settled) return;
        if (control?.cancelled) {
          try {
            if (typeof streamRequest.cancel === 'function') {
              streamRequest.cancel();
            }
          } catch (error) {
            console.error('Failed to cancel POI stream request after abort', error);
          }
          settleReject(control?.cancelReason);
          return;
        }
        if (!totalRows) {
          totalRows = Number(row?.TotalRows || 0);
          onProgress({ phase: 'count-complete', totalRows, rowsRead: 0 }, { force: true });
        }
        rows.push(normalizePoiPoint(row));
        rowsRead += 1;
        const now = Date.now();
        const shouldEmit = rowsRead === 1
          || rowsRead === totalRows
          || (rowsRead - lastEmittedRows) >= 250
          || (now - lastEmittedAt) >= 250;
        if (!shouldEmit) return;
        lastEmittedRows = rowsRead;
        lastEmittedAt = now;
        onProgress({ phase: 'stream', totalRows, rowsRead });
      });
      streamRequest.on('error', err => {
        if (settled) return;
        if (control?.cancelled || isPoiRunCancellationError(err)) {
          settleReject(control?.cancelReason);
          return;
        }
        settled = true;
        if (cancelSubscription) {
          cancelSubscription();
          cancelSubscription = null;
        }
        reject(err);
      });
      streamRequest.on('done', () => {
        if (settled) return;
        if (control?.cancelled) {
          settleReject(control?.cancelReason);
          return;
        }
        if (!totalRows) {
          onProgress({ phase: 'count-complete', totalRows: 0, rowsRead: 0 }, { force: true });
        }
        onProgress({ phase: 'stream-complete', totalRows, rowsRead }, { force: true });
        settleResolve();
      });
      control?.bindRequest(streamRequest);
      control?.throwIfCancelled();
      streamRequest.query(buildPoiSourceQuery(targetTable, normalizedFilters));
    });

    control?.throwIfCancelled();
    return { rows, targetTable: config.tableName, totalRows: totalRows || rows.length };
  } finally {
    pool.close();
  }
}
function createPoiRunProgressReporter(totalPoints, onProgress) {
  const total = Math.max(1, Number(totalPoints) || 0);
  let lastEmitAt = 0;
  let lastSignature = '';

  return (snapshot = {}, options = {}) => {
    const normalized = {
      phase: snapshot.phase || 'dbscan',
      totalPoints: total,
      scannedPoints: Math.max(0, Math.min(total, Number(snapshot.scannedPoints) || 0)),
      neighborhoodsCalculated: Math.max(0, Number(snapshot.neighborhoodsCalculated) || 0),
      clustersFound: Math.max(0, Number(snapshot.clustersFound) || 0),
      assignedPointCount: Math.max(0, Math.min(total, Number(snapshot.assignedPointCount) || 0)),
      noisePointCount: Math.max(0, Math.min(total, Number(snapshot.noisePointCount) || 0)),
      queueProcessed: Math.max(0, Number(snapshot.queueProcessed) || 0),
      currentClusterSize: Math.max(0, Number(snapshot.currentClusterSize) || 0),
      currentPointIndex: Math.max(0, Math.min(total, Number(snapshot.currentPointIndex) || 0))
    };
    const signature = JSON.stringify(normalized);
    const now = Date.now();
    if (!options.force && signature === lastSignature) return;
    if (!options.force && now - lastEmitAt < 200) return;
    lastEmitAt = now;
    lastSignature = signature;
    onProgress(normalized);
  };
}
function createEventLoopYieldController(maxBlockMs = 40) {
  let nextYieldAt = Date.now() + maxBlockMs;
  return async (force = false) => {
    if (!force && Date.now() < nextYieldAt) return false;
    await new Promise(resolve => setImmediate(resolve));
    nextYieldAt = Date.now() + maxBlockMs;
    return true;
  };
}
async function runDbscan(points, epsKm, minPoints, onProgress = () => {}, control = null) {
  const clusters = [];
  const visited = new Array(points.length).fill(false);
  const assigned = new Array(points.length).fill(-1);
  const neighborCache = new Map();
  let neighborhoodCount = 0;
  let visitedCount = 0;
  let assignedPointCount = 0;
  let noiseCount = 0;
  let queueProcessed = 0;
  const maybeYield = createEventLoopYieldController();
  const yieldAndCheck = async (force = false) => {
    await maybeYield(force);
    control?.throwIfCancelled();
  };

  function setAssigned(index, value) {
    const previous = assigned[index];
    if (previous === value) return;
    if (previous >= 0 && value < 0) assignedPointCount -= 1;
    if (previous !== -2 && value === -2) noiseCount += 1;
    if (previous === -2 && value !== -2) noiseCount -= 1;
    if (previous < 0 && value >= 0) assignedPointCount += 1;
    assigned[index] = value;
  }

  function emitProgress(snapshot = {}, options = {}) {
    onProgress({
      phase: snapshot.phase || 'dbscan',
      totalPoints: points.length,
      scannedPoints: snapshot.scannedPoints ?? visitedCount,
      neighborhoodsCalculated: snapshot.neighborhoodsCalculated ?? neighborhoodCount,
      clustersFound: snapshot.clustersFound ?? clusters.length,
      assignedPointCount: snapshot.assignedPointCount ?? assignedPointCount,
      noisePointCount: snapshot.noisePointCount ?? noiseCount,
      queueProcessed: snapshot.queueProcessed ?? queueProcessed,
      currentClusterSize: snapshot.currentClusterSize ?? 0,
      currentPointIndex: snapshot.currentPointIndex ?? 0
    }, options);
  }

  async function regionQuery(index) {
    control?.throwIfCancelled();
    if (neighborCache.has(index)) return neighborCache.get(index);
    const neighbors = [];
    for (let otherIndex = 0; otherIndex < points.length; otherIndex += 1) {
      control?.throwIfCancelled();
      if (haversineKm(points[index], points[otherIndex]) <= epsKm) {
        neighbors.push(otherIndex);
      }
      if ((otherIndex % 256) === 0) {
        await yieldAndCheck();
      }
    }
    neighborCache.set(index, neighbors);
    neighborhoodCount += 1;
    emitProgress({
      phase: 'scan',
      currentPointIndex: index + 1
    });
    await yieldAndCheck();
    return neighbors;
  }

  emitProgress({ phase: 'scan', currentPointIndex: 0 }, { force: true });

  for (let pointIndex = 0; pointIndex < points.length; pointIndex += 1) {
    control?.throwIfCancelled();
    if ((pointIndex % 16) === 0) {
      await yieldAndCheck();
    }
    if (visited[pointIndex]) continue;
    visited[pointIndex] = true;
    visitedCount += 1;
    const neighbors = await regionQuery(pointIndex);

    if (neighbors.length < minPoints) {
      setAssigned(pointIndex, -2);
      emitProgress({
        phase: 'scan',
        currentPointIndex: pointIndex + 1
      });
      continue;
    }

    const clusterId = clusters.length;
    const cluster = [];
    const queue = neighbors.filter(index => index !== pointIndex);
    const queued = new Set(queue);

    setAssigned(pointIndex, clusterId);
    cluster.push(pointIndex);
    emitProgress({
      phase: 'expand',
      currentPointIndex: pointIndex + 1,
      currentClusterSize: cluster.length
    });

    for (let queueIndex = 0; queueIndex < queue.length; queueIndex += 1) {
      control?.throwIfCancelled();
      if ((queueIndex % 128) === 0) {
        await yieldAndCheck();
      }
      const neighborIndex = queue[queueIndex];
      queueProcessed += 1;
      if (!visited[neighborIndex]) {
        visited[neighborIndex] = true;
        visitedCount += 1;
        const secondaryNeighbors = await regionQuery(neighborIndex);
        if (secondaryNeighbors.length >= minPoints) {
          secondaryNeighbors.forEach(candidate => {
            if (!queued.has(candidate) && candidate !== pointIndex) {
              queued.add(candidate);
              queue.push(candidate);
            }
          });
        }
      }

      if (assigned[neighborIndex] === -1 || assigned[neighborIndex] === -2) {
        setAssigned(neighborIndex, clusterId);
        cluster.push(neighborIndex);
      }

      emitProgress({
        phase: 'expand',
        currentPointIndex: pointIndex + 1,
        currentClusterSize: cluster.length
      });
    }

    clusters.push(cluster);
    emitProgress({
      phase: 'cluster-finalized',
      currentPointIndex: pointIndex + 1,
      currentClusterSize: cluster.length,
      clustersFound: clusters.length
    }, { force: true });
  }

  emitProgress({
    phase: 'completed',
    currentPointIndex: points.length,
    currentClusterSize: 0
  }, { force: true });
  await yieldAndCheck(true);

  const noiseIndexes = [];
  for (let index = 0; index < assigned.length; index += 1) {
    control?.throwIfCancelled();
    if (assigned[index] === -2) noiseIndexes.push(index);
  }

  return {
    clusters,
    noiseCount,
    noiseIndexes,
    stats: {
      totalPoints: points.length,
      scannedPoints: visitedCount,
      neighborhoodsCalculated: neighborhoodCount,
      assignedPointCount,
      queueProcessed,
      noisePointCount: noiseCount
    }
  };
}
function buildClusterPayload(points, indexes, clusterId) {
  const clusterPoints = indexes.map(index => points[index]);
  const latitudes = clusterPoints.map(point => point.Latitudine);
  const longitudes = clusterPoints.map(point => point.Longitudine);
  const totalLat = latitudes.reduce((sum, value) => sum + value, 0);
  const totalLon = longitudes.reduce((sum, value) => sum + value, 0);
  const compactness = calculateClusterCompactness(clusterPoints);

  return {
    ClusterId: clusterId,
    PointCount: clusterPoints.length,
    CentroLatitudine: totalLat / clusterPoints.length,
    CentroLongitudine: totalLon / clusterPoints.length,
    MinLatitudine: Math.min(...latitudes),
    MaxLatitudine: Math.max(...latitudes),
    MinLongitudine: Math.min(...longitudes),
    MaxLongitudine: Math.max(...longitudes),
    CompattezzaPercentuale: compactness.compactnessPercent,
    DistanzaMediaKm: compactness.meanDistanceKm,
    RaggioRiferimentoKm: compactness.referenceRadiusKm,
    Punti: clusterPoints
  };
}
function buildNoisePayload(points, indexes) {
  const noisePoints = indexes.map(index => points[index]);
  return {
    PointCount: noisePoints.length,
    Punti: noisePoints
  };
}
async function writePoiClusterFiles(runId, setName, clusters, points, noiseIndexes = [], control = null, onProgress = () => {}) {
  const runDirName = `${new Date().toISOString().replace(/[:.]/g, '-')}_${sanitizePathSegment(setName)}_${sanitizePathSegment(runId).slice(0, 8)}`;
  const runDir = path.join(POI_CLUSTER_DIR, runDirName);
  fs.mkdirSync(runDir, { recursive: true });
  if (control) control.runDir = runDir;
  control?.throwIfCancelled();
  const noisePayload = buildNoisePayload(points, noiseIndexes);
  const noiseFileName = 'noise-points.json';
  const noiseFilePath = path.join(runDir, noiseFileName);
  saveJson(noiseFilePath, noisePayload);

  const files = [];
  onProgress({ filesWritten: 0, filesTotal: clusters.length });

  for (let clusterIndex = 0; clusterIndex < clusters.length; clusterIndex += 1) {
    control?.throwIfCancelled();
    const indexes = clusters[clusterIndex];
    const payload = buildClusterPayload(points, indexes, clusterIndex + 1);
    const fileName = `cluster-${String(clusterIndex + 1).padStart(4, '0')}.json`;
    const filePath = path.join(runDir, fileName);
    saveJson(filePath, payload);
    files.push({
      clusterId: payload.ClusterId,
      pointCount: payload.PointCount,
      compactnessPercent: payload.CompattezzaPercentuale,
      meanDistanceKm: payload.DistanzaMediaKm,
      referenceRadiusKm: payload.RaggioRiferimentoKm,
      fileName,
      path: path.relative(__dirname, filePath)
    });
    onProgress({ filesWritten: clusterIndex + 1, filesTotal: clusters.length });
    await new Promise(resolve => setImmediate(resolve));
  }

  return {
    directory: path.relative(__dirname, runDir),
    files,
    noise: {
      pointCount: noisePayload.PointCount,
      fileName: noiseFileName,
      path: path.relative(__dirname, noiseFilePath)
    }
  };
}
function summarizeExecutionSet(set = {}) {
  return {
    id: set.id || null,
    name: String(set.name || ''),
    dbscan: {
      eps: set.dbscan?.eps ?? null,
      minPoints: set.dbscan?.minPoints ?? null,
      unit: 'km'
    },
    filters: {
      citta: String(set.filters?.citta || ''),
      idtipoinfrazione: normalizeInfractionIds(set.filters?.idtipoinfrazione)
    }
  };
}
function getPoiRunById(id) {
  const runId = String(id || '').trim();
  if (!runId) return null;
  return getPoiRuns().find(item => item.id === runId) || null;
}
function resolvePoiRunArtifactsDirectory(run) {
  const directory = run?.artifacts?.directory;
  if (!directory) return null;
  const absolutePath = path.resolve(__dirname, String(directory));
  const allowedPrefix = `${path.resolve(POI_CLUSTER_DIR)}${path.sep}`;
  if (!absolutePath.startsWith(allowedPrefix)) return null;
  return { absolutePath };
}
function resolvePoiClusterFile(run, clusterId) {
  const numericClusterId = Number(clusterId);
  if (!Number.isFinite(numericClusterId)) return null;
  const files = Array.isArray(run?.artifacts?.files) ? run.artifacts.files : [];
  const file = files.find(item => Number(item.clusterId) === numericClusterId) || null;
  if (!file?.path) return null;
  const absolutePath = path.resolve(__dirname, String(file.path));
  const allowedPrefix = `${path.resolve(POI_CLUSTER_DIR)}${path.sep}`;
  if (!absolutePath.startsWith(allowedPrefix)) return null;
  return {
    file,
    absolutePath
  };
}
function resolvePoiNoiseFile(run) {
  const noise = run?.artifacts?.noise || null;
  if (!noise?.path) return null;
  const absolutePath = path.resolve(__dirname, String(noise.path));
  const allowedPrefix = `${path.resolve(POI_CLUSTER_DIR)}${path.sep}`;
  if (!absolutePath.startsWith(allowedPrefix)) return null;
  return {
    absolutePath
  };
}
async function executePoiRun(executionSet) {
  const startedAt = new Date().toISOString();
  const poiConfig = getPoiConfig();
  if (!poiConfig?.sqlConnectionString) throw new Error('Missing POI SQL connection string');
  if (!poiConfig?.tableName) throw new Error('Missing POI source table name');

  const runId = crypto.randomUUID();
  const runEntry = {
    id: runId,
    setId: executionSet.id,
    setName: executionSet.name,
    status: 'running',
    startedAt,
    finishedAt: null,
    elapsedMs: null,
    elapsedLabel: null,
    durationMs: null,
    durationLabel: null,
    message: 'Esecuzione avviata',
    sourceTable: poiConfig.tableName,
    executionSet: summarizeExecutionSet(executionSet),
    summary: null,
    artifacts: null,
    error: null
  };
  const control = createPoiRunControl();
  currentPoiRunControl = control;

  updatePoiRunState({
    running: true,
    status: 'running',
    progress: 2,
    message: 'Validazione configurazione',
    stage: 'validation',
    runId,
    setId: executionSet.id,
    setName: executionSet.name,
    startedAt,
    finishedAt: null,
    summary: null,
    details: null,
    error: null
  });

  try {
    updatePoiRunState({
      progress: 8,
      message: 'Avvio lettura dati dal database',
      stage: 'database',
      details: {
        totalPoints: null,
        rowsRead: 0,
        scannedPoints: 0,
        neighborhoodsCalculated: 0,
        clustersFound: 0,
        assignedPointCount: 0,
        noisePointCount: 0,
        queueProcessed: 0,
        currentClusterSize: 0,
        filesWritten: 0,
        filesTotal: 0
      }
    });
    updatePoiRunState({
      progress: 12,
      message: 'Attesa primo blocco dati dal database',
      stage: 'database'
    });
    const { rows, totalRows } = await loadPoiSourcePoints(poiConfig, executionSet, progress => {
      const total = Math.max(0, Number(progress.totalRows) || 0);
      if (progress.phase === 'count-complete') {
        updatePoiRunState({
          progress: total ? 14 : 100,
          message: `Conteggio completato: ${total} punti da leggere`,
          stage: 'database-count-complete',
          details: {
            totalPoints: total,
            rowsRead: 0,
            scannedPoints: 0,
            neighborhoodsCalculated: 0,
            clustersFound: 0,
            assignedPointCount: 0,
            noisePointCount: 0,
            queueProcessed: 0,
            currentClusterSize: 0,
            filesWritten: 0,
            filesTotal: 0
          }
        });
        return;
      }

      const rowsRead = Math.max(0, Number(progress.rowsRead) || 0);
      const ratio = total > 0 ? rowsRead / total : 1;
      const current = total > 0 ? 15 + Math.round(ratio * 15) : 30;
      updatePoiRunState({
        progress: current,
        message: `Lettura dati dal database: ${rowsRead} di ${total}`,
        stage: 'database-stream',
        details: {
          totalPoints: total,
          rowsRead,
          scannedPoints: 0,
          neighborhoodsCalculated: 0,
          clustersFound: 0,
          assignedPointCount: 0,
          noisePointCount: 0,
          queueProcessed: 0,
          currentClusterSize: 0,
          filesWritten: 0,
          filesTotal: 0
        }
      });
    }, control);
    updatePoiRunState({
      progress: rows.length ? 30 : 100,
      message: `Lettura completata: ${rows.length} punti trovati`,
      stage: 'database-complete',
      details: {
        totalPoints: totalRows || rows.length,
        rowsRead: rows.length,
        scannedPoints: 0,
        neighborhoodsCalculated: 0,
        clustersFound: 0,
        assignedPointCount: 0,
        noisePointCount: 0,
        queueProcessed: 0,
        currentClusterSize: 0,
        filesWritten: 0,
        filesTotal: 0
      }
    });
    if (!rows.length) {
      const finishedAt = new Date().toISOString();
      const timing = getRunTiming(startedAt, finishedAt);
      const summary = {
        sourcePointCount: 0,
        clusterCount: 0,
        clusteredPointCount: 0,
        noisePointCount: 0
      };
      const completed = {
        ...runEntry,
        status: 'completed',
        finishedAt,
        elapsedMs: timing.durationMs,
        elapsedLabel: timing.durationLabel,
        durationMs: timing.durationMs,
        durationLabel: timing.durationLabel,
        message: 'Nessun dato trovato con i filtri selezionati',
        summary,
        artifacts: {
          directory: null,
          files: []
        }
      };
      appendPoiRun(completed);
      updatePoiRunState({
        running: false,
        status: 'completed',
        progress: 100,
        message: completed.message,
        stage: 'completed',
        finishedAt,
        summary,
        details: {
          totalPoints: 0,
          rowsRead: 0,
          scannedPoints: 0,
          neighborhoodsCalculated: 0,
          clustersFound: 0,
          assignedPointCount: 0,
          noisePointCount: 0,
          queueProcessed: 0,
          currentClusterSize: 0,
          filesWritten: 0,
          filesTotal: 0
        },
        error: null
      });
      return completed;
    }

    const reportDbscanProgress = createPoiRunProgressReporter(rows.length, progress => {
      const total = Math.max(1, progress.totalPoints || rows.length);
      const scanRatio = (progress.scannedPoints || 0) / total;
      const neighborRatio = Math.min(1, (progress.neighborhoodsCalculated || 0) / total);
      const assignmentRatio = (progress.assignedPointCount || 0) / total;
      const ratio = Math.min(1, (scanRatio * 0.45) + (neighborRatio * 0.35) + (assignmentRatio * 0.2));
      const current = 32 + Math.round(ratio * 50);
      const isExpanding = progress.phase === 'expand';
      const action = isExpanding ? 'Analisi punti' : 'Analisi punti';
      const confirmedClusterCount = progress.clustersFound || 0;
      const visibleClusterCount = confirmedClusterCount + (progress.currentClusterSize > 0 ? 1 : 0);
      const clusterText = progress.currentClusterSize > 0
        ? `${visibleClusterCount} cluster (${confirmedClusterCount} confermati)`
        : `${visibleClusterCount} cluster`;
      updatePoiRunState({
        progress: current,
        stage: isExpanding ? 'dbscan-expand' : 'dbscan-scan',
        message: `${action} ${progress.scannedPoints} di ${total} · ${clusterText}`,
        details: {
          totalPoints: total,
          rowsRead: total,
          scannedPoints: progress.scannedPoints || 0,
          neighborhoodsCalculated: progress.neighborhoodsCalculated || 0,
          clustersFound: progress.clustersFound || 0,
          assignedPointCount: progress.assignedPointCount || 0,
          noisePointCount: progress.noisePointCount || 0,
          queueProcessed: progress.queueProcessed || 0,
          currentClusterSize: progress.currentClusterSize || 0,
          filesWritten: 0,
          filesTotal: 0
        }
      });
    });
    updatePoiRunState({
      progress: 32,
      message: `DBSCAN avviato su ${rows.length} punti`,
      stage: 'dbscan',
      details: {
        totalPoints: rows.length,
        rowsRead: rows.length,
        scannedPoints: 0,
        neighborhoodsCalculated: 0,
        clustersFound: 0,
        assignedPointCount: 0,
        noisePointCount: 0,
        queueProcessed: 0,
        currentClusterSize: 0,
        filesWritten: 0,
        filesTotal: 0
      }
    });
    const { clusters, noiseCount, stats, noiseIndexes } = await runDbscan(
      rows,
      Number(executionSet.dbscan?.eps),
      Number(executionSet.dbscan?.minPoints),
      reportDbscanProgress,
      control
    );

    const clusteredPointCount = clusters.reduce((sum, cluster) => sum + cluster.length, 0);
    updatePoiRunState({
      progress: 84,
      message: `DBSCAN completato: ${clusters.length} cluster trovati`,
      stage: 'dbscan-complete',
      details: {
        totalPoints: rows.length,
        rowsRead: rows.length,
        scannedPoints: stats.scannedPoints,
        neighborhoodsCalculated: stats.neighborhoodsCalculated,
        clustersFound: clusters.length,
        assignedPointCount: clusteredPointCount,
        noisePointCount: noiseCount,
        queueProcessed: stats.queueProcessed,
        currentClusterSize: 0,
        filesWritten: 0,
        filesTotal: clusters.length
      }
    });
    updatePoiRunState({
      progress: 86,
      message: `Scrittura file cluster (${clusters.length})`,
      stage: 'writing',
      details: {
        totalPoints: rows.length,
        rowsRead: rows.length,
        scannedPoints: stats.scannedPoints,
        neighborhoodsCalculated: stats.neighborhoodsCalculated,
        clustersFound: clusters.length,
        assignedPointCount: clusteredPointCount,
        noisePointCount: noiseCount,
        queueProcessed: stats.queueProcessed,
        currentClusterSize: 0,
        filesWritten: 0,
        filesTotal: clusters.length
      }
    });
    const artifacts = await writePoiClusterFiles(
      runId,
      executionSet.name,
      clusters,
      rows,
      noiseIndexes,
      control,
      progress => {
        const ratio = progress.filesTotal > 0 ? progress.filesWritten / progress.filesTotal : 0;
        updatePoiRunState({
          progress: 86 + Math.round(ratio * 11),
          message: progress.filesTotal > 0
            ? `Scrittura file cluster (${progress.filesWritten}/${progress.filesTotal})`
            : 'Preparazione file cluster',
          stage: 'writing',
          details: {
            totalPoints: rows.length,
            rowsRead: rows.length,
            scannedPoints: stats.scannedPoints,
            neighborhoodsCalculated: stats.neighborhoodsCalculated,
            clustersFound: clusters.length,
            assignedPointCount: clusteredPointCount,
            noisePointCount: noiseCount,
            queueProcessed: stats.queueProcessed,
            currentClusterSize: 0,
            filesWritten: progress.filesWritten,
            filesTotal: progress.filesTotal
          }
        });
      }
    );

    const summary = {
      sourcePointCount: rows.length,
      clusterCount: clusters.length,
      clusteredPointCount,
      noisePointCount: noiseCount
    };
    const finishedAt = new Date().toISOString();
    const timing = getRunTiming(startedAt, finishedAt);
    const completed = {
      ...runEntry,
      status: 'completed',
      finishedAt,
      elapsedMs: timing.durationMs,
      elapsedLabel: timing.durationLabel,
      durationMs: timing.durationMs,
      durationLabel: timing.durationLabel,
      message: `Esecuzione completata: ${clusters.length} cluster generati`,
      summary,
      artifacts
    };

    appendPoiRun(completed);
    updatePoiRunState({
      running: false,
      status: 'completed',
      progress: 100,
      message: completed.message,
      stage: 'completed',
      finishedAt,
      summary,
      details: {
        totalPoints: rows.length,
        rowsRead: rows.length,
        scannedPoints: stats.scannedPoints,
        neighborhoodsCalculated: stats.neighborhoodsCalculated,
        clustersFound: clusters.length,
        assignedPointCount: clusteredPointCount,
        noisePointCount: noiseCount,
        queueProcessed: stats.queueProcessed,
        currentClusterSize: 0,
        filesWritten: artifacts.files.length,
        filesTotal: artifacts.files.length
      },
      error: null
    });
    return completed;
  } catch (err) {
    const finishedAt = new Date().toISOString();
    const timing = getRunTiming(startedAt, finishedAt);
    if (control?.cancelled || isPoiRunCancellationError(err)) {
      const reason = control?.cancelReason || String(err?.message || err);
      if (control?.runDir) {
        try {
          fs.rmSync(control.runDir, { recursive: true, force: true });
        } catch (cleanupError) {
          console.error('Failed to clean up cancelled POI run directory', cleanupError);
        }
      }
      const cancelled = {
        ...runEntry,
        status: 'cancelled',
        finishedAt,
        elapsedMs: timing.durationMs,
        elapsedLabel: timing.durationLabel,
        durationMs: timing.durationMs,
        durationLabel: timing.durationLabel,
        message: reason,
        summary: null,
        artifacts: null,
        error: { message: reason }
      };
      const currentState = getPoiRunState();
      appendPoiRun(cancelled);
      updatePoiRunState({
        running: false,
        status: 'cancelled',
        progress: currentState.progress ?? 100,
        message: reason,
        stage: 'cancelled',
        finishedAt,
        summary: null,
        details: currentState.details || null,
        error: cancelled.error
      });
      return cancelled;
    }
    const failed = {
      ...runEntry,
      status: 'failed',
      finishedAt,
      elapsedMs: timing.durationMs,
      elapsedLabel: timing.durationLabel,
      durationMs: timing.durationMs,
      durationLabel: timing.durationLabel,
      message: String(err?.message || err),
      error: serializeSqlError(err) || { message: String(err?.message || err) }
    };
    appendPoiRun(failed);
    updatePoiRunState({
      running: false,
      status: 'failed',
      progress: 100,
      message: failed.message,
      stage: 'failed',
      finishedAt,
      summary: null,
      details: null,
      error: failed.error
    });
    throw err;
  } finally {
    if (currentPoiRunControl === control) {
      currentPoiRunControl = null;
    }
  }
}
function normalizeReportTableName(tableName) {
  const raw = String(tableName || '').trim();
  if (!raw) throw new Error('Missing destination table');
  const parts = raw.split('.').map(part => part.trim()).filter(Boolean);
  if (parts.length < 1 || parts.length > 2) {
    throw new Error('Destination table must be in "table" or "schema.table" format');
  }
  return parts.map(part => `[${part.replace(/]/g, ']]')}]`).join('.');
}
function buildSqlExportRows(result) {
  const issues = Array.isArray(result?.issues) ? result.issues : [];
  return issues.map(issue => ({
    key: String(issue?.key || '').trim(),
    summary: String(issue?.summary || ''),
    status: String(issue?.status || ''),
    assignee: String(issue?.assignee || ''),
    updated: String(issue?.updated || ''),
    priority: String(issue?.priority || ''),
    issuetype: String(issue?.issuetype || '')
  })).filter(row => row.key);
}
function parseSqlConnectionConfig(connectionString) {
  const raw = String(connectionString || '').trim();
  if (!raw) throw new Error('Missing SQL Server connection string');
  const config = sql.ConnectionPool.parseConnectionString(raw);
  if (!config || typeof config.server !== 'string' || !config.server.trim()) {
    throw new Error('Invalid SQL Server connection string: missing Server');
  }
  return config;
}
function buildMergeSql(targetTable) {
  const targetColumns = SQL_EXPORT_COLUMNS.map(c => `[${c}]`).join(', ');
  const sourceColumns = SQL_EXPORT_COLUMNS.map(c => `source.[${c}]`).join(', ');
  const updateSet = SQL_EXPORT_COLUMNS
    .filter(c => c !== 'key')
    .map(c => `target.[${c}] = source.[${c}]`)
    .join(', ');
  return `
    MERGE ${targetTable} AS target
    USING (VALUES __VALUES_PLACEHOLDER__) AS source (${targetColumns})
    ON target.[key] = source.[key]
    WHEN MATCHED THEN
      UPDATE SET ${updateSet}
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (${targetColumns})
      VALUES (${sourceColumns})
    WHEN NOT MATCHED BY SOURCE THEN
      DELETE;
  `;
}
function normalizeScheduleTime(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const match = raw.match(/^([01]\d|2[0-3]):([0-5]\d)$/);
  if (!match) return '';
  return `${match[1]}:${match[2]}`;
}
function getLocalDateKey(date = new Date()) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}
function getLocalTimeKey(date = new Date()) {
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  return `${hours}:${minutes}`;
}
function getScheduleExecutionKey(date = new Date(), time = '') {
  const normalizedTime = normalizeScheduleTime(time);
  return normalizedTime ? `${getLocalDateKey(date)}@${normalizedTime}` : '';
}
function appendScheduleLog(entry) {
  const log = getScheduleLog();
  const entries = Array.isArray(log.entries) ? log.entries : [];
  entries.unshift(entry);
  setScheduleLog({ entries: entries.slice(0, SCHEDULE_LOG_LIMIT) });
}
function readScheduledReports(cfg) {
  const reports = getReports().filter(report => Boolean(report?.scheduledEnabled));
  return reports.filter(report => {
    if (!report?.exportEnabled) return false;
    if (!String(report?.table || '').trim()) return false;
    return true;
  });
}
function buildScheduleSummary(results) {
  return results.reduce((acc, item) => {
    acc.total += 1;
    if (item.status === 'merged') acc.merged += 1;
    else if (item.status === 'skipped') acc.skipped += 1;
    else if (item.status === 'failed') acc.failed += 1;
    return acc;
  }, { total: 0, merged: 0, skipped: 0, failed: 0 });
}
async function executeScheduledDbMerges({ mode = 'manual', scheduleKey = null } = {}) {
  const state = getScheduleState();
  if (state.running) {
    const err = new Error('A scheduled execution is already running');
    err.status = 409;
    throw err;
  }

  const startedAt = new Date().toISOString();
  const cfg = getConfig() || {};
  const scheduledTime = normalizeScheduleTime(cfg.dailyScheduleTime || '');
  const runKey = scheduleKey || (mode === 'auto' ? getScheduleExecutionKey(new Date(), scheduledTime) : `${startedAt}#manual`);
  if (mode === 'auto' && !scheduledTime) {
    const entry = {
      id: crypto.randomUUID(),
      mode,
      startedAt,
      finishedAt: new Date().toISOString(),
      scheduleTime: '',
      executionKey: runKey,
      status: 'skipped',
      message: 'No daily schedule time configured',
      summary: { total: 0, merged: 0, skipped: 0, failed: 0 },
      results: []
    };
    appendScheduleLog(entry);
    return entry;
  }

  setScheduleState({ ...state, running: true, currentRunKey: runKey });

  const entry = {
    id: crypto.randomUUID(),
    mode,
    startedAt,
    finishedAt: null,
    scheduleTime: scheduledTime || '',
    executionKey: runKey,
    status: 'running',
    message: '',
    summary: { total: 0, merged: 0, skipped: 0, failed: 0 },
    results: []
  };

  try {
    const reports = getReports().filter(report => Boolean(report?.scheduledEnabled));
    if (!reports.length) {
      entry.status = 'skipped';
      entry.message = 'No reports are marked for scheduled execution';
      entry.finishedAt = new Date().toISOString();
      appendScheduleLog(entry);
      if (mode === 'auto') {
        setScheduleState({ running: false, lastAutoRunKey: runKey, lastRunAt: entry.finishedAt, currentRunKey: null });
      } else {
        setScheduleState({ ...getScheduleState(), running: false, currentRunKey: null, lastRunAt: entry.finishedAt });
      }
      return entry;
    }

    for (const report of reports) {
      const resultEntry = {
        reportId: report.id,
        title: report.title,
        group: report.group || '',
        status: 'pending',
        message: '',
        mergedRows: 0,
        targetTable: report.table || ''
      };
      try {
        const search = await jiraSearchAll(cfg, report);
        if (!report.exportEnabled || !String(report.table || '').trim()) {
          resultEntry.status = 'skipped';
          resultEntry.message = 'Report not configured for DB export';
        } else {
          const mergeResult = await syncReportToSqlServer(cfg, report, search);
          resultEntry.status = 'merged';
          resultEntry.message = 'DB merge completed';
          resultEntry.mergedRows = mergeResult.mergedRows || 0;
          resultEntry.targetTable = mergeResult.targetTable || resultEntry.targetTable;
        }
      } catch (err) {
        resultEntry.status = 'failed';
        resultEntry.message = String(err?.message || err);
        resultEntry.details = err?.body || serializeSqlError(err);
      }
      entry.results.push(resultEntry);
    }

    entry.summary = buildScheduleSummary(entry.results);
    entry.status = entry.summary.failed > 0 ? (entry.summary.merged > 0 ? 'partial' : 'failed') : 'ok';
    entry.message = entry.status === 'ok'
      ? 'Scheduled execution completed'
      : entry.status === 'partial'
        ? 'Scheduled execution completed with errors'
        : 'Scheduled execution failed';
    entry.finishedAt = new Date().toISOString();
    appendScheduleLog(entry);

    if (mode === 'auto') {
      setScheduleState({ running: false, lastAutoRunKey: runKey, lastRunAt: entry.finishedAt, currentRunKey: null });
    } else {
      setScheduleState({ ...getScheduleState(), running: false, currentRunKey: null, lastRunAt: entry.finishedAt, lastManualRunAt: entry.finishedAt });
    }
    return entry;
  } catch (err) {
    entry.status = 'failed';
    entry.message = String(err?.message || err);
    entry.finishedAt = new Date().toISOString();
    appendScheduleLog(entry);
    setScheduleState({ ...getScheduleState(), running: false, currentRunKey: null });
    throw err;
  }
}
async function maybeRunAutoSchedule() {
  const cfg = getConfig() || {};
  const scheduledTime = normalizeScheduleTime(cfg.dailyScheduleTime || '');
  if (!scheduledTime) return;
  const now = new Date();
  if (getLocalTimeKey(now) !== scheduledTime) return;
  const executionKey = getScheduleExecutionKey(now, scheduledTime);
  const state = getScheduleState();
  if (state.running || state.lastAutoRunKey === executionKey) return;
  try {
    await executeScheduledDbMerges({ mode: 'auto', scheduleKey: executionKey });
  } catch (err) {
    console.error('Scheduled execution failed', err);
  }
}
function serializeSqlError(err, seen = new WeakSet(), depth = 0) {
  if (!err || typeof err !== 'object') return null;
  if (seen.has(err) || depth > 6) return { name: err.name || 'Error', message: err.message || String(err) };
  seen.add(err);

  const details = {
    name: err.name || 'Error',
    message: err.message || String(err),
    code: err.code || null,
    number: err.number || null,
    state: err.state || null,
    class: err.class || null,
    lineNumber: err.lineNumber || null,
    serverName: err.serverName || null,
    procName: err.procName || null,
    line: err.line || null
  };

  if (err.originalError && typeof err.originalError === 'object') {
    details.originalError = serializeSqlError(err.originalError, seen, depth + 1);
  }
  if (Array.isArray(err.errors) && err.errors.length) {
    details.errors = err.errors.map(item => serializeSqlError(item, seen, depth + 1)).filter(Boolean);
  }
  if (Array.isArray(err.precedingErrors) && err.precedingErrors.length) {
    details.precedingErrors = err.precedingErrors.map(item => serializeSqlError(item, seen, depth + 1)).filter(Boolean);
  }
  if (err.cause && typeof err.cause === 'object') {
    details.cause = serializeSqlError(err.cause, seen, depth + 1);
  }
  return details;
}
async function syncReportToSqlServer(cfg, report, result) {
  if (!report?.exportEnabled) throw new Error('Export is disabled for this report');
  if (!String(report?.table || '').trim()) throw new Error('Missing destination table');

  const rows = buildSqlExportRows(result);
  const targetTable = normalizeReportTableName(report.table);
  const connectionConfig = parseSqlConnectionConfig(cfg?.sqlServerConnectionString);

  const pool = await new sql.ConnectionPool(connectionConfig).connect();

  const transaction = new sql.Transaction(pool);
  try {
    await transaction.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);
    if (rows.length) {
      const request = new sql.Request(transaction);
      const valuesSql = rows.map((row, rowIndex) => {
        return `(${SQL_EXPORT_COLUMNS.map((column, columnIndex) => {
          const paramName = `r${rowIndex}_${columnIndex}`;
          request.input(paramName, sql.NVarChar(sql.MAX), row[column]);
          return `@${paramName}`;
        }).join(', ')})`;
      }).join(',\n');
      const mergeSql = buildMergeSql(targetTable).replace('__VALUES_PLACEHOLDER__', valuesSql);
      await request.query(mergeSql);
    } else {
      await new sql.Request(transaction).query(`DELETE FROM ${targetTable};`);
    }

    await transaction.commit();
    return { ok: true, mergedRows: rows.length, targetTable: report.table };
  } catch (err) {
    try { await transaction.rollback(); } catch {}
    throw err;
  } finally {
    pool.close();
  }
}
async function testSqlConnection(connectionString) {
  const pool = await new sql.ConnectionPool(parseSqlConnectionConfig(connectionString)).connect();
  try {
    const result = await pool.request().query('SELECT 1 AS ok');
    return {
      ok: true,
      connected: true,
      result: result?.recordset?.[0]?.ok ?? 1
    };
  } finally {
    pool.close();
  }
}
function authHeader(cfg) {
  return 'Basic ' + Buffer.from(`${cfg.jiraUser}:${cfg.jiraToken}`).toString('base64');
}
function normalizeBaseUrl(url) {
  const raw = String(url || '').trim().replace(/\/$/, '');
  if (!raw) return '';
  if (/^https?:\/\//i.test(raw)) return raw;
  return `https://${raw}`;
}
async function jiraFetch(cfg, jiraPath, options = {}) {
  const base = normalizeBaseUrl(cfg.jiraBaseUrl);
  if (!base) throw new Error('Missing Jira base URL');
  const url = `${base}/rest/api/${cfg.jiraApiVersion || '3'}${jiraPath}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      Accept: 'application/json',
      Authorization: authHeader(cfg),
      ...(options.headers || {})
    }
  });
  const text = await res.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch { body = text; }
  if (!res.ok) {
    const msg = body && typeof body === 'object' ? (body.errorMessages?.join(', ') || body.message || text) : text;
    const err = new Error(`Jira ${res.status}: ${msg}`);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  return body;
}

app.get('/health', (_req, res) => res.json({ ok: true }));

app.get('/api/config', (_req, res) => res.json(getConfig() || {}));
app.post('/api/config', (req, res) => {
  const cfg = req.body || {};
  setConfig(cfg);
  res.json({ ok: true });
});

app.post('/api/config/test', async (req, res) => {
  const cfg = req.body || getConfig();
  if (!cfg?.jiraBaseUrl || !cfg?.jiraUser || !cfg?.jiraToken) {
    return res.status(400).json({ ok: false, error: 'Missing Jira config' });
  }
  try {
    const myself = await jiraFetch(cfg, '/myself');
    res.json({ ok: true, myself });
  } catch (err) {
    res.status(err.status || 500).json({ ok: false, error: err.message, body: err.body });
  }
});

app.post('/api/config/test-sql', async (req, res) => {
  const cfg = req.body || getConfig();
  try {
    const result = await testSqlConnection(cfg?.sqlServerConnectionString);
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(err.status || 500).json({
      ok: false,
      error: err.message,
      body: err.body || null,
      details: serializeSqlError(err)
    });
  }
});

app.get('/api/poi/config', (_req, res) => {
  res.json({ ok: true, config: getPoiConfig() });
});

app.post('/api/poi/config', (req, res) => {
  const next = normalizePoiConfig(req.body || {});
  setPoiConfig(next);
  res.json({ ok: true, config: next });
});

app.post('/api/poi/config/test-sql', async (req, res) => {
  const source = req.body || getPoiConfig();
  try {
    const result = await testSqlConnection(source?.sqlConnectionString);
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(err.status || 500).json({
      ok: false,
      error: err.message,
      body: err.body || null,
      details: serializeSqlError(err)
    });
  }
});

app.get('/api/poi/execution-sets', (_req, res) => {
  res.json({ ok: true, sets: getExecutionSets() });
});

app.post('/api/poi/execution-sets', (req, res) => {
  try {
    const next = normalizeExecutionSet(req.body || {});
    const sets = getExecutionSets();
    sets.unshift(next);
    setExecutionSets(sets);
    res.json({ ok: true, set: next });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.put('/api/poi/execution-sets/:id', (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing set id' });

  const sets = getExecutionSets();
  const idx = sets.findIndex(set => set.id === id);
  if (idx < 0) return res.status(404).json({ ok: false, error: 'Set not found' });

  try {
    const updated = normalizeExecutionSet({ ...req.body, id }, sets[idx]);
    const next = [updated, ...sets.filter(set => set.id !== id)];
    setExecutionSets(next);
    res.json({ ok: true, set: updated });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.delete('/api/poi/execution-sets/:id', (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing set id' });

  const sets = getExecutionSets();
  const next = sets.filter(set => set.id !== id);
  if (next.length === sets.length) return res.status(404).json({ ok: false, error: 'Set not found' });

  setExecutionSets(next);
  res.json({ ok: true });
});

app.get('/api/poi/runs', (_req, res) => {
  res.json({
    ok: true,
    runs: getPoiRuns(),
    current: getPoiRunState()
  });
});

app.get('/api/poi/run-state', (_req, res) => {
  res.json({ ok: true, state: getPoiRunState() });
});

app.post('/api/poi/run-state/cancel', (_req, res) => {
  const state = getPoiRunState();
  if (!state.running) {
    return res.status(409).json({ ok: false, error: 'No running execution to cancel' });
  }
  if (!currentPoiRunControl) {
    return res.status(409).json({ ok: false, error: 'Current execution is not cancelable' });
  }

  currentPoiRunControl.abort('Esecuzione interrotta dall\'utente');
  updatePoiRunState({
    message: 'Interruzione richiesta...',
    stage: 'cancelling'
  });

  res.json({
    ok: true,
    accepted: true,
    state: getPoiRunState()
  });
});

app.get('/api/poi/runs/:id', (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing run id' });

  const run = getPoiRunById(id);
  if (!run) return res.status(404).json({ ok: false, error: 'Run not found' });
  res.json({ ok: true, run });
});

app.delete('/api/poi/runs/:id', (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing run id' });

  const runs = getPoiRuns();
  const run = runs.find(item => item.id === id);
  if (!run) return res.status(404).json({ ok: false, error: 'Run not found' });
  if (getPoiRunState()?.running && getPoiRunState()?.runId === id) {
    return res.status(409).json({ ok: false, error: 'Cannot delete a running execution' });
  }

  const nextRuns = runs.filter(item => item.id !== id);
  const resolvedDir = resolvePoiRunArtifactsDirectory(run);
  if (resolvedDir?.absolutePath && fs.existsSync(resolvedDir.absolutePath)) {
    try {
      fs.rmSync(resolvedDir.absolutePath, { recursive: true, force: true });
    } catch (error) {
      console.error('Failed to remove POI run artifacts directory', error);
      return res.status(500).json({ ok: false, error: 'Failed to remove run artifacts' });
    }
  }

  setPoiRuns(nextRuns);
  res.json({ ok: true });
});

app.get('/api/poi/runs/:id/clusters/:clusterId', (req, res) => {
  const id = String(req.params.id || '').trim();
  const clusterId = String(req.params.clusterId || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing run id' });
  if (!clusterId) return res.status(400).json({ ok: false, error: 'Missing cluster id' });

  const run = getPoiRunById(id);
  if (!run) return res.status(404).json({ ok: false, error: 'Run not found' });

  const resolved = resolvePoiClusterFile(run, clusterId);
  if (!resolved) return res.status(404).json({ ok: false, error: 'Cluster file not found' });
  if (!fs.existsSync(resolved.absolutePath)) {
    return res.status(404).json({ ok: false, error: 'Cluster file missing on disk' });
  }

  const cluster = loadJson(resolved.absolutePath, null);
  if (!cluster || typeof cluster !== 'object') {
    return res.status(500).json({ ok: false, error: 'Cluster file is not readable' });
  }
  const metrics = calculateClusterCompactness(Array.isArray(cluster.Punti) ? cluster.Punti : []);
  const enrichedCluster = {
    ...cluster,
    CompattezzaPercentuale: Number.isFinite(Number(cluster.CompattezzaPercentuale)) ? Number(cluster.CompattezzaPercentuale) : metrics.compactnessPercent,
    DistanzaMediaKm: Number.isFinite(Number(cluster.DistanzaMediaKm)) ? Number(cluster.DistanzaMediaKm) : metrics.meanDistanceKm,
    RaggioRiferimentoKm: Number.isFinite(Number(cluster.RaggioRiferimentoKm)) ? Number(cluster.RaggioRiferimentoKm) : metrics.referenceRadiusKm
  };
  if (
    enrichedCluster.CompattezzaPercentuale !== cluster.CompattezzaPercentuale ||
    enrichedCluster.DistanzaMediaKm !== cluster.DistanzaMediaKm ||
    enrichedCluster.RaggioRiferimentoKm !== cluster.RaggioRiferimentoKm
  ) {
    saveJson(resolved.absolutePath, enrichedCluster);
  }

  if (String(req.query.raw || '').toLowerCase() === '1' || String(req.query.raw || '').toLowerCase() === 'true') {
    return res.json(enrichedCluster);
  }

  res.json({
    ok: true,
    cluster: enrichedCluster
  });
});

app.get('/api/poi/runs/:id/noise', (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing run id' });

  const run = getPoiRunById(id);
  if (!run) return res.status(404).json({ ok: false, error: 'Run not found' });

  const resolved = resolvePoiNoiseFile(run);
  if (!resolved) return res.status(404).json({ ok: false, error: 'Noise file not found' });
  if (!fs.existsSync(resolved.absolutePath)) {
    return res.status(404).json({ ok: false, error: 'Noise file missing on disk' });
  }

  const noise = loadJson(resolved.absolutePath, null);
  if (!noise || typeof noise !== 'object') {
    return res.status(500).json({ ok: false, error: 'Noise file is not readable' });
  }

  if (String(req.query.raw || '').toLowerCase() === '1' || String(req.query.raw || '').toLowerCase() === 'true') {
    return res.json(noise);
  }

  res.json({
    ok: true,
    noise
  });
});

app.post('/api/poi/execution-sets/:id/run', async (req, res) => {
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'Missing set id' });

  const currentState = getPoiRunState();
  if (currentState.running) {
    return res.status(409).json({
      ok: false,
      error: `An execution is already running for "${currentState.setName || currentState.setId || 'unknown'}"`
    });
  }

  const executionSet = getExecutionSets().find(item => item.id === id);
  if (!executionSet) return res.status(404).json({ ok: false, error: 'Set not found' });

  const normalizedSet = normalizeExecutionSet(executionSet, executionSet);
  const poiConfig = getPoiConfig();
  try {
    if (!poiConfig?.sqlConnectionString) throw new Error('Missing POI SQL connection string');
    if (!poiConfig?.tableName) throw new Error('Missing POI source table name');
    parseSqlConnectionConfig(poiConfig.sqlConnectionString);
    normalizeReportTableName(poiConfig.tableName);

    executePoiRun(normalizedSet).catch(err => {
      console.error('POI execution failed', err);
    });
    res.status(202).json({
      ok: true,
      accepted: true,
      set: summarizeExecutionSet(normalizedSet),
      state: getPoiRunState()
    });
  } catch (err) {
    res.status(err.status || 500).json({
      ok: false,
      error: err.message,
      details: err.body || serializeSqlError(err)
    });
  }
});

app.get('/api/reports', (_req, res) => res.json(getReports()));
app.post('/api/reports', (req, res) => {
  const reports = getReports();
  const report = req.body || {};
  if (!report.title || !report.jql) return res.status(400).json({ ok: false, error: 'title and jql required' });
  if (report.exportEnabled && !String(report.table || '').trim()) {
    return res.status(400).json({ ok: false, error: 'table required when export is enabled' });
  }
  const next = {
    id: report.id || crypto.randomUUID(),
    title: report.title,
    group: report.group || '',
    jql: report.jql,
    exportEnabled: Boolean(report.exportEnabled),
    table: String(report.table || '').trim(),
    scheduledEnabled: Boolean(report.scheduledEnabled)
  };
  reports.push(next);
  setReports(reports);
  res.json(next);
});
app.put('/api/reports/:id', (req, res) => {
  const reports = getReports();
  const idx = reports.findIndex(r => r.id === req.params.id);
  if (idx < 0) return res.status(404).json({ ok: false, error: 'not found' });
  if (req.body?.exportEnabled && !String(req.body?.table || '').trim()) {
    return res.status(400).json({ ok: false, error: 'table required when export is enabled' });
  }
  reports[idx] = {
    ...reports[idx],
    ...req.body,
    exportEnabled: Boolean(req.body?.exportEnabled),
    scheduledEnabled: Boolean(req.body?.scheduledEnabled),
    table: String(req.body?.table || '').trim(),
    group: String(req.body?.group || ''),
    id: reports[idx].id
  };
  setReports(reports);
  res.json(reports[idx]);
});
app.delete('/api/reports/:id', (req, res) => {
  const reports = getReports().filter(r => r.id !== req.params.id);
  setReports(reports);
  res.json({ ok: true });
});

function jiraSearchFields() {
  return ['summary', 'status', 'assignee', 'updated', 'priority', 'issuetype'];
}

function buildJiraSearchRequest(cfg, report, nextPageToken = null) {
  const base = normalizeBaseUrl(cfg.jiraBaseUrl);
  const apiVersion = cfg.jiraApiVersion || '3';
  const url = `${base}/rest/api/${apiVersion}/search/jql`;
  const body = {
    jql: report.jql,
    maxResults: Number(cfg.jiraPageSize || 50),
    fields: jiraSearchFields()
  };
  if (nextPageToken) body.nextPageToken = nextPageToken;
  return { url, body };
}

async function jiraSearchAll(cfg, report) {
  const issues = [];
  let total = null;
  let nextPageToken = null;

  do {
    const request = buildJiraSearchRequest(cfg, report, nextPageToken);
    const response = await fetch(request.url, {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        Authorization: authHeader(cfg)
      },
      body: JSON.stringify(request.body)
    });

    const text = await response.text();
    let search = null;
    try { search = text ? JSON.parse(text) : null; } catch { search = text; }

    if (!response.ok) {
      const msg = search && typeof search === 'object' ? (search.errorMessages?.join(', ') || search.message || text) : text;
      throw Object.assign(new Error(`Jira ${response.status}: ${msg}`), { status: response.status, body: search });
    }

    total = typeof search?.total === 'number' ? search.total : total;
    issues.push(...(search?.issues || []));
    nextPageToken = search?.nextPageToken || null;
  } while (nextPageToken);

  return { total: total ?? issues.length, issues };
}

app.post('/api/reports/test', (req, res) => {
  const cfg = req.body?.config || getConfig();
  const reportIds = Array.isArray(req.body?.reportIds) ? req.body.reportIds : [];
  const allReports = getReports();
  const reports = allReports.filter(r => reportIds.includes(r.id));
  if (!reports.length) {
    return res.status(400).json({
      ok: false,
      error: 'No reports selected',
      debug: { reportIds, availableReportIds: allReports.map(r => r.id) }
    });
  }
  const preview = reports.map(report => ({ id: report.id, title: report.title, group: report.group || '', jql: report.jql, request: buildJiraSearchRequest(cfg, report) }));
  res.json({ ok: true, preview });
});

app.post('/api/reports/run', async (req, res) => {
  const cfg = req.body?.config || getConfig();
  const reportIds = Array.isArray(req.body?.reportIds) ? req.body.reportIds : [];
  const allReports = getReports();
  const reports = allReports.filter(r => reportIds.includes(r.id));
  if (!reports.length) {
    return res.status(400).json({
      ok: false,
      error: 'No reports selected',
      debug: { reportIds, availableReportIds: allReports.map(r => r.id) }
    });
  }
  try {
    const results = [];
    for (const report of reports) {
      const search = await jiraSearchAll(cfg, report);
      results.push({
        id: report.id,
        title: report.title,
        group: report.group || '',
        jql: report.jql,
        count: search.total || 0,
        issues: (search.issues || []).map(x => ({
          key: x.key,
          summary: x.fields?.summary || '',
          status: x.fields?.status?.name || '',
          assignee: x.fields?.assignee?.displayName || x.fields?.assignee?.emailAddress || '',
          updated: x.fields?.updated || '',
          priority: x.fields?.priority?.name || '',
          issuetype: x.fields?.issuetype?.name || ''
        }))
      });
    }
    res.json({ ok: true, results });
  } catch (err) {
    res.status(err.status || 500).json({ ok: false, error: err.message, body: err.body, details: err.body || null });
  }
});

app.post('/api/reports/:id/db-merge', async (req, res) => {
  const cfg = req.body?.config || getConfig();
  const reportId = String(req.params.id || '').trim();
  const result = req.body?.result || null;
  if (!reportId) return res.status(400).json({ ok: false, error: 'Missing report id' });
  if (!result || typeof result !== 'object') {
    return res.status(400).json({ ok: false, error: 'Missing report result payload' });
  }
  if (result.dryRun) {
    return res.status(400).json({ ok: false, error: 'DB merge is not available for dry-run results' });
  }
  const report = getReports().find(r => r.id === reportId);
  if (!report) return res.status(404).json({ ok: false, error: 'Report not found' });
  try {
    const mergeResult = await syncReportToSqlServer(cfg, report, result);
    res.json({ ok: true, ...mergeResult });
  } catch (err) {
    res.status(err.status || 500).json({
      ok: false,
      error: err.message,
      details: err.body || serializeSqlError(err)
    });
  }
});

app.get('/api/schedule/log', (_req, res) => {
  const state = getScheduleState();
  const log = getScheduleLog();
  res.json({
    ok: true,
    running: Boolean(state.running),
    lastAutoRunKey: state.lastAutoRunKey || null,
    lastRunAt: state.lastRunAt || null,
    entries: Array.isArray(log.entries) ? log.entries : []
  });
});

app.post('/api/schedule/run', async (_req, res) => {
  try {
    const entry = await executeScheduledDbMerges({ mode: 'manual' });
    res.json({ ok: true, entry });
  } catch (err) {
    res.status(err.status || 500).json({
      ok: false,
      error: err.message,
      details: err.body || serializeSqlError(err)
    });
  }
});

app.get('/api/issues/:key/status-history', async (req, res) => {
  const cfg = getConfig();
  const issueKey = String(req.params.key || '').trim();
  if (!cfg?.jiraBaseUrl || !cfg?.jiraUser || !cfg?.jiraToken) {
    return res.status(400).json({ ok: false, error: 'Missing Jira config' });
  }
  if (!issueKey) {
    return res.status(400).json({ ok: false, error: 'Missing issue key' });
  }
  try {
    const issue = await jiraFetch(cfg, `/issue/${encodeURIComponent(issueKey)}?fields=summary,status&expand=changelog`);
    const histories = Array.isArray(issue?.changelog?.histories) ? issue.changelog.histories : [];
    const events = histories
      .flatMap(history => {
        const items = Array.isArray(history?.items) ? history.items : [];
        return items
          .filter(item => item?.field === 'status')
          .map(item => ({
            at: history.created || '',
            author: history.author?.displayName || history.author?.emailAddress || history.author?.accountId || 'Sconosciuto',
            from: item.fromString || '—',
            to: item.toString || '—'
          }));
      })
      .sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime());

    res.json({
      ok: true,
      issue: {
        key: issue.key,
        summary: issue.fields?.summary || '',
        currentStatus: issue.fields?.status?.name || ''
      },
      events
    });
  } catch (err) {
    res.status(err.status || 500).json({ ok: false, error: err.message, body: err.body, details: err.body || null });
  }
});

setInterval(() => {
  maybeRunAutoSchedule();
}, 30_000);
setTimeout(() => {
  maybeRunAutoSchedule();
}, 5_000);

app.listen(PORT, () => {
  console.log(`POI studio running on http://localhost:${PORT}`);
});
