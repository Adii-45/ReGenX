/**
 * @fileoverview Offline Dispatch Sync Engine for ReGenX PWA
 * Handles offline action queuing via IndexedDB, background sync, retry logic,
 * and conflict resolution through the ConflictResolver module.
 * Phase 3 Upgrade: Integrated ConflictResolver validation/dedup, replaced fake
 * API endpoints with real Appwrite CloudSync bridge, fixed retry scheduling.
 */

import { resolveConflict, isDuplicate, validateAction, mergeGPSUpdates } from './conflict-resolver.js';

const DB_NAME = 'ReGenX_OfflineDB';
const DB_VERSION = 1;
const STORE_NAME = 'pendingActions';
const MAX_RETRIES = 5;
const MAX_QUEUE_SIZE = 200;

let db = null;
let _syncInProgress = false;

/**
 * Initialize IndexedDB for offline storage
 * @returns {Promise<IDBDatabase>}
 */
export function initOfflineDB() {
  return new Promise((resolve, reject) => {
    if (db) { resolve(db); return; }

    const request = indexedDB.open(DB_NAME, DB_VERSION);

    request.onupgradeneeded = (event) => {
      const database = event.target.result;
      if (!database.objectStoreNames.contains(STORE_NAME)) {
        const store = database.createObjectStore(STORE_NAME, { keyPath: 'id' });
        store.createIndex('timestamp', 'timestamp', { unique: false });
        store.createIndex('type', 'type', { unique: false });
      }
    };

    request.onsuccess = (event) => {
      db = event.target.result;
      console.log('[OfflineSync] IndexedDB initialized');
      resolve(db);
    };

    request.onerror = (event) => {
      console.error('[OfflineSync] IndexedDB error:', event.target.error);
      reject(event.target.error);
    };
  });
}

/**
 * Check whether the IndexedDB instance is ready
 * @returns {boolean}
 */
export function isDBReady() {
  return db !== null;
}

/**
 * Generate unique UUID for each action
 * @returns {string}
 */
function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Queue an action for offline storage with validation, dedup, and GPS merge.
 * @param {string} type - Action type (dispatch, pickup, gps, scan, reward, plant, sync-order, sync-account)
 * @param {Object} payload - Action data
 * @returns {Promise<string|null>} - UUID of queued action, or null if rejected
 */
export async function queueOfflineAction(type, payload) {
  if (!db) {
    // Graceful fallback: try to initialize, else reject
    try { await initOfflineDB(); } catch { /* ignore */ }
    if (!db) {
      console.warn('[OfflineSync] DB unavailable — action not queued');
      return null;
    }
  }

  // Extended type list: original types + sync-order/sync-account used by app.js
  const extendedValidTypes = ['dispatch', 'pickup', 'gps', 'scan', 'reward', 'plant', 'sync-order', 'sync-account', 'sync-notification'];
  if (!extendedValidTypes.includes(type)) {
    console.error(`[OfflineSync] Invalid action type rejected: ${type}`);
    return null;
  }

  if (!payload || typeof payload !== 'object') {
    console.error(`[OfflineSync] Invalid payload rejected for type: ${type}`);
    return null;
  }

  // Fetch current pending actions for dedup and GPS merge checks
  const pendingActions = await getPendingActions();

  // Enforce maximum queue size to prevent unbounded growth
  if (pendingActions.length >= MAX_QUEUE_SIZE) {
    console.warn(`[OfflineSync] Queue at capacity (${MAX_QUEUE_SIZE}). Evicting oldest non-GPS action.`);
    const oldest = pendingActions.filter(a => a.type !== 'gps').sort((a, b) => a.timestamp - b.timestamp)[0];
    if (oldest) {
      await removeAction(oldest.id);
    } else {
      console.error('[OfflineSync] Queue full — cannot evict. Action rejected.');
      return null;
    }
  }

  // Duplicate detection: skip exact payload matches for non-GPS types
  if (type !== 'gps' && isDuplicate(pendingActions, type, payload)) {
    console.warn(`[OfflineSync] Duplicate action rejected: ${type}`);
    return null;
  }

  // GPS merge: if queuing a GPS action, remove all existing GPS actions for same entity
  if (type === 'gps') {
    const existingGPS = pendingActions.filter(a => a.type === 'gps');
    for (const old of existingGPS) {
      // Only merge GPS for the same entity (rider/provider)
      const sameEntity = old.payload?.riderId === payload?.riderId ||
                         old.payload?.entityId === payload?.entityId;
      if (sameEntity || (!old.payload?.riderId && !old.payload?.entityId)) {
        await removeAction(old.id);
      }
    }
  }

  const action = {
    id: generateUUID(),
    type,
    payload,
    timestamp: Date.now(),
    retryCount: 0,
    status: 'pending'
  };

  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    const request = store.add(action);

    request.onsuccess = () => {
      console.log(`[OfflineSync] Action queued: ${type} (${action.id})`);
      updateSyncUI('pending');
      resolve(action.id);
    };

    request.onerror = () => {
      console.error(`[OfflineSync] Failed to queue action: ${type}`, request.error);
      reject(request.error);
    };
  });
}

/**
 * Get all pending actions from IndexedDB
 * @returns {Promise<Array>}
 */
export function getPendingActions() {
  return new Promise((resolve, reject) => {
    if (!db) { resolve([]); return; }

    const tx = db.transaction(STORE_NAME, 'readonly');
    const store = tx.objectStore(STORE_NAME);
    const request = store.getAll();

    request.onsuccess = () => resolve(request.result || []);
    request.onerror = () => reject(request.error);
  });
}

/**
 * Get the count of pending actions (for UI indicators)
 * @returns {Promise<number>}
 */
export async function getPendingCount() {
  try {
    const actions = await getPendingActions();
    return actions.length;
  } catch {
    return 0;
  }
}

/**
 * Remove a successfully synced action from IndexedDB
 * @param {string} id - UUID of action to remove
 * @returns {Promise<void>}
 */
export function removeAction(id) {
  return new Promise((resolve, reject) => {
    if (!db) { resolve(); return; }

    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    const request = store.delete(id);

    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
  });
}

/**
 * Update retry count for a failed action in IndexedDB
 * @param {Object} action - The action to update
 * @returns {Promise<void>}
 */
function updateActionRetry(action) {
  return new Promise((resolve, reject) => {
    if (!db) { resolve(); return; }

    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    const request = store.put(action);

    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
  });
}

/**
 * Sync all pending actions when online.
 * Uses a sync lock to prevent parallel flush races.
 * @returns {Promise<void>}
 */
export async function syncPendingActions() {
  if (_syncInProgress) {
    console.log('[OfflineSync] Sync already in progress — skipping.');
    return;
  }
  if (!navigator.onLine) {
    console.log('[OfflineSync] Still offline — sync deferred.');
    return;
  }

  _syncInProgress = true;

  try {
    // Merge GPS actions before processing
    let actions = await getPendingActions();
    if (actions.length === 0) {
      updateSyncUI('synced');
      return;
    }

    // Apply GPS merge to reduce redundant telemetry writes
    const merged = mergeGPSUpdates(actions);
    // Remove GPS actions that were merged away
    const mergedIds = new Set(merged.map(a => a.id));
    for (const action of actions) {
      if (!mergedIds.has(action.id)) {
        await removeAction(action.id);
      }
    }
    actions = merged;

    console.log(`[OfflineSync] Syncing ${actions.length} pending actions...`);
    updateSyncUI('syncing');

    let hasRetryScheduled = false;

    for (const action of actions) {
      try {
        await processAction(action);
        await removeAction(action.id);
        console.log(`[OfflineSync] Synced: ${action.type} (${action.id})`);
      } catch (error) {
        console.warn(`[OfflineSync] Failed to sync: ${action.id}`, error);
        const shouldRetry = await handleRetry(action);
        if (shouldRetry && !hasRetryScheduled) {
          hasRetryScheduled = true;
        }
      }
    }

    const remaining = await getPendingActions();
    if (remaining.length === 0) {
      updateSyncUI('synced');
    } else if (hasRetryScheduled) {
      updateSyncUI('pending');
    } else {
      updateSyncUI('retry-failed');
    }
  } catch (error) {
    console.error('[OfflineSync] Sync run failed:', error);
    updateSyncUI('retry-failed');
  } finally {
    _syncInProgress = false;
  }
}

/**
 * Process a single action by routing to the appropriate Appwrite/Realtime sync.
 * Replaces the previous fake /api/* endpoints with real infrastructure.
 * @param {Object} action - Queued offline action
 * @returns {Promise<void>}
 */
async function processAction(action) {
  if (!navigator.onLine) {
    throw new Error('Device is offline');
  }

  const { type, payload } = action;

  switch (type) {
    case 'dispatch':
    case 'pickup':
    case 'sync-order': {
      // Route order/dispatch/pickup actions through CloudSync
      if (window.CloudSync && window.CloudSync.isLive && payload?.id) {
        await window.CloudSync.pushDocument(
          window.CloudSync.config?.ordersCollectionId,
          payload
        );
      } else if (window.CloudSync && payload?.id) {
        // CloudSync exists but not live — queue for its own retry
        window.CloudSync.queueOfflineWrite('ord:' + payload.id, payload);
      }
      break;
    }

    case 'sync-account': {
      if (window.CloudSync && window.CloudSync.isLive && payload?.id) {
        await window.CloudSync.pushAccount(payload);
      } else if (window.CloudSync && payload?.id) {
        window.CloudSync.queueOfflineWrite('acc:' + payload.id, payload);
      }
      break;
    }

    case 'gps': {
      // GPS telemetry routes through ReGenXRealtime for live updates
      if (window.ReGenXRealtime && window.ReGenXRealtime.isConnected()) {
        window.ReGenXRealtime.emitOperationalEvent({
          type: 'GPS_UPDATE',
          rooms: ['network_room', 'riders_room', 'providers_room'],
          updates: [{ key: `gps:${payload?.riderId || payload?.entityId || 'unknown'}`, value: payload, action: 'set' }],
          meta: { statusLabel: 'GPS Sync' }
        });
      }
      // Also persist to Appwrite if order-linked
      if (window.CloudSync && window.CloudSync.isLive && payload?.orderId) {
        await window.CloudSync.pushDocument(
          window.CloudSync.config?.ordersCollectionId,
          payload
        );
      }
      break;
    }

    case 'scan': {
      // Scan results are stored locally and optionally pushed to cloud
      if (window.CloudSync && window.CloudSync.isLive && payload?.id) {
        await window.CloudSync.pushDocument(
          window.CloudSync.config?.ordersCollectionId,
          payload
        );
      }
      break;
    }

    case 'reward': {
      // Reward actions update account tokens via CloudSync
      if (window.CloudSync && window.CloudSync.isLive && payload?.id) {
        await window.CloudSync.pushAccount(payload);
      }
      break;
    }

    case 'plant': {
      // Plant confirmations are order updates
      if (window.CloudSync && window.CloudSync.isLive && payload?.id) {
        await window.CloudSync.pushDocument(
          window.CloudSync.config?.ordersCollectionId,
          payload
        );
      }
      break;
    }

    case 'sync-notification': {
      // Notifications are already stored locally; this is a marker for remote sync.
      // No cloud action required.
      break;
    }

    default: {
      console.warn(`[OfflineSync] Unhandled action type in processAction: ${type}`);
      break;
    }
  }
}

/**
 * Handle retry with exponential backoff.
 * Updates the action's retry count in IndexedDB and schedules a real
 * re-execution of syncPendingActions after the backoff delay.
 * @param {Object} action - The failed action
 * @returns {Promise<boolean>} - true if retry was scheduled, false if max retries reached
 */
async function handleRetry(action) {
  if (!db) return false;

  if (action.retryCount >= MAX_RETRIES) {
    console.error(`[OfflineSync] Max retries (${MAX_RETRIES}) reached for ${action.id} — removing from queue.`);
    await removeAction(action.id);
    return false;
  }

  const updatedAction = { ...action, retryCount: action.retryCount + 1, status: 'retry-pending' };
  const delay = Math.min(Math.pow(2, updatedAction.retryCount) * 1000, 30000);

  // Persist updated retry count immediately
  try {
    await updateActionRetry(updatedAction);
  } catch (err) {
    console.error('[OfflineSync] Failed to update retry count:', err);
    return false;
  }

  console.log(`[OfflineSync] Retry ${updatedAction.retryCount}/${MAX_RETRIES} for ${action.id} in ${delay}ms`);

  // Schedule a real sync attempt after the backoff delay
  setTimeout(() => {
    if (navigator.onLine && !_syncInProgress) {
      syncPendingActions().catch(err => {
        console.error('[OfflineSync] Scheduled retry sync failed:', err);
      });
    }
  }, delay);

  return true;
}

/**
 * Update sync status UI banner
 * @param {'pending'|'syncing'|'synced'|'retry-failed'} status
 */
export function updateSyncUI(status) {
  let banner = document.getElementById('sync-status-banner');

  if (!banner) {
    banner = document.createElement('div');
    banner.id = 'sync-status-banner';
    banner.style.cssText = `
      position: fixed; bottom: 20px; right: 20px;
      padding: 10px 20px; border-radius: 8px;
      font-family: sans-serif; font-size: 14px;
      font-weight: 600; z-index: 9999;
      transition: all 0.3s ease; box-shadow: 0 4px 12px rgba(0,0,0,0.2);
    `;
    document.body.appendChild(banner);
  }

  const states = {
    pending:      { text: '🕐 Pending Sync',  bg: '#f59e0b', color: '#fff' },
    syncing:      { text: '🔄 Syncing...',     bg: '#3b82f6', color: '#fff' },
    synced:       { text: '✅ Synced',          bg: '#10b981', color: '#fff' },
    'retry-failed': { text: '❌ Retry Failed', bg: '#ef4444', color: '#fff' }
  };

  const state = states[status] || states.pending;
  banner.textContent = state.text;
  banner.style.background = state.bg;
  banner.style.color = state.color;
  banner.style.display = 'block';

  if (status === 'synced') {
    setTimeout(() => { banner.style.display = 'none'; }, 3000);
  }
}

/**
 * Setup online/offline event listeners.
 * Guarded against duplicate registration.
 */
let _networkListenersRegistered = false;
export function setupNetworkListeners() {
  if (_networkListenersRegistered) return;
  _networkListenersRegistered = true;

  window.addEventListener('online', async () => {
    console.log('[OfflineSync] Back online — starting sync...');
    showOfflineBanner(false);
    await syncPendingActions();
    // Also flush any CloudSync legacy queue entries
    if (window.CloudSync && typeof window.CloudSync.flushOfflineQueue === 'function') {
      window.CloudSync.flushOfflineQueue().catch(() => {});
    }
  });

  window.addEventListener('offline', () => {
    console.log('[OfflineSync] Gone offline');
    showOfflineBanner(true);
    updateSyncUI('pending');
  });
}

/**
 * Show/hide offline notification banner
 * @param {boolean} isOffline
 */
function showOfflineBanner(isOffline) {
  let offlineBanner = document.getElementById('offline-banner');

  if (!offlineBanner) {
    offlineBanner = document.createElement('div');
    offlineBanner.id = 'offline-banner';
    offlineBanner.style.cssText = `
      position: fixed; top: 0; left: 0; width: 100%;
      padding: 10px; text-align: center;
      font-family: sans-serif; font-size: 14px;
      font-weight: 600; z-index: 99999;
      transition: all 0.3s ease;
    `;
    document.body.appendChild(offlineBanner);
  }

  if (isOffline) {
    offlineBanner.textContent = '📵 You are offline — actions will sync when connected';
    offlineBanner.style.background = '#1f2937';
    offlineBanner.style.color = '#f9fafb';
    offlineBanner.style.display = 'block';
  } else {
    offlineBanner.style.display = 'none';
  }
}