/**
 * @fileoverview ReGenX Appwrite Cloud Sync Engine
 * Handles real-time synchronization between LocalStorage and Appwrite Cloud Databases.
 * Integrates WebSockets for Live Dispatch updates.
 * Phase 3 Upgrade: Integrated timestamp-based conflict resolution via ConflictResolver,
 * replaced legacy localStorage queue with IndexedDB bridge from offline-sync.js.
 * @author GSSoC Contributor
 */

import { resolveConflict } from './conflict-resolver.js';
import { queueOfflineAction as idbQueueAction, isDBReady } from './offline-sync.js';

const STORAGE_KEY_PREFIX = "regenx-v3:";

/**
 * Retrieves a local order from LocalStorage.
 * @param {string} id - The order ID.
 * @returns {Object|null} The order object or null if not found/error.
 */
function getLocalOrder(id) {
    try {
        const val = localStorage.getItem(STORAGE_KEY_PREFIX + 'ord:' + id);
        return val ? JSON.parse(val) : null;
    } catch (e) {
        return null;
    }
}

export const CloudSync = {
    client: null,
    databases: null,
    isLive: false,
    config: null,
    unsubscribe: null,

    /**
     * Loads configurations from the .env file at runtime.
     * @returns {Promise<Object>} The parsed configuration object.
     */
    loadConfig: async () => {
        const config = {
            endpoint: 'https://cloud.appwrite.io/v1',
            projectId: '',
            databaseId: '',
            ordersCollectionId: '',
            accountsCollectionId: ''
        };
        try {
            const response = await fetch('/.env');
            if (response.ok) {
                const text = await response.text();
                const lines = text.split(/\r?\n/);
                for (const line of lines) {
                    const trimmed = line.trim();
                    if (!trimmed || trimmed.startsWith('#')) continue;
                    
                    const eqIndex = trimmed.indexOf('=');
                    if (eqIndex === -1) continue;
                    
                    const key = trimmed.substring(0, eqIndex).trim();
                    let val = trimmed.substring(eqIndex + 1).trim();
                    
                    // Strip quotes if present
                    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
                        val = val.substring(1, val.length - 1);
                    }
                    
                    if (key === 'VITE_APPWRITE_ENDPOINT' || key === 'APPWRITE_ENDPOINT') {
                        config.endpoint = val;
                    } else if (key === 'VITE_APPWRITE_PROJECT_ID' || key === 'APPWRITE_PROJECT_ID') {
                        config.projectId = val;
                    } else if (key === 'VITE_APPWRITE_DATABASE_ID' || key === 'APPWRITE_DATABASE_ID') {
                        config.databaseId = val;
                   } else if (key === 'VITE_APPWRITE_COLLECTION_ID_ORDERS' || key === 'APPWRITE_COLLECTION_ID_ORDERS') {
                        config.ordersCollectionId = val;
                    } else if (key === 'VITE_APPWRITE_COLLECTION_ID_ACCOUNTS' || key === 'APPWRITE_COLLECTION_ID_ACCOUNTS') {
                        config.accountsCollectionId = val;
                    }
                }
            } else {
                console.warn("Could not load /.env file, status:", response.status);
            }
        } catch (e) {
            console.warn("Failed to fetch or parse .env file. Falling back to defaults.", e);
        }

        // Check window process for fallback
        if (window.process && window.process.env) {
            config.endpoint = window.process.env.VITE_APPWRITE_ENDPOINT || window.process.env.APPWRITE_ENDPOINT || config.endpoint;
            config.projectId = window.process.env.VITE_APPWRITE_PROJECT_ID || window.process.env.APPWRITE_PROJECT_ID || config.projectId;
            config.databaseId = window.process.env.VITE_APPWRITE_DATABASE_ID || window.process.env.APPWRITE_DATABASE_ID || config.databaseId;
            config.ordersCollectionId = window.process.env.VITE_APPWRITE_COLLECTION_ID_ORDERS || window.process.env.APPWRITE_COLLECTION_ID_ORDERS || config.ordersCollectionId;
        }

        return config;
    },

    /**
     * Initializes the Appwrite Web SDK and establishes Realtime connection.
     * @returns {Promise<void>}
     */
    init: async () => {
        if (!window.Appwrite) {
            console.warn("Appwrite SDK not loaded.");
            CloudSync.renderSyncBadge('offline', 'Offline');
            return;
        }

        try {
            const config = await CloudSync.loadConfig();
            
            if (!config.projectId || !config.databaseId || !config.ordersCollectionId) {
                console.warn("Appwrite credentials not fully configured in .env. Running in local mode.");
                CloudSync.isLive = false;
                CloudSync.renderSyncBadge('local', 'Local Mode');
                return;
            }

            const { Client, Databases } = window.Appwrite;
            
            CloudSync.client = new Client()
                .setEndpoint(config.endpoint) 
                .setProject(config.projectId);

            CloudSync.databases = new Databases(CloudSync.client);
            CloudSync.isLive = true;
            CloudSync.config = config;


            CloudSync.renderSyncBadge('live', 'Cloud Live');
            
            // Setup Realtime Subscription
            CloudSync.subscribeToDispatches();
        } catch (e) {
            console.error("CloudSync Init Failed:", e);
            CloudSync.isLive = false;
            CloudSync.renderSyncBadge('error', 'Sync Error');
        }
    },

    /**
     * Renders a premium Glassmorphic sync badge in the DOM header.
     * @param {string} [status='local'] - The status ('live', 'local', 'error', 'offline', or 'syncing').
     * @param {string} [label='Local Mode'] - The label to display.
     * @returns {void}
     */
    renderSyncBadge: (status = 'local', label = 'Local Mode') => {
        const topbarUser = document.querySelector('.topbar-user');
        const header = document.querySelector('header');
        if (!topbarUser && !header) return;

        let dotColor = '#64748b'; // Slate
        let borderColor = 'rgba(100, 116, 139, 0.2)';
        let bgStyle = 'background: rgba(100, 116, 139, 0.05);';
        let pulseAnim = '';

        if (status === 'live') {
            dotColor = '#10b981'; // Green
            borderColor = 'rgba(16, 185, 129, 0.3)';
            bgStyle = 'background: rgba(16, 185, 129, 0.1); backdrop-filter: blur(10px);';
            pulseAnim = 'animation: cs-pulse 2s infinite;';
        } else if (status === 'syncing') {
            dotColor = '#f59e0b'; // Amber
            borderColor = 'rgba(245, 158, 11, 0.3)';
            bgStyle = 'background: rgba(245, 158, 11, 0.1); backdrop-filter: blur(10px);';
            pulseAnim = 'animation: cs-pulse 0.8s infinite;';
        } else if (status === 'local') {
            dotColor = '#6366f1'; // Indigo/Blue
            borderColor = 'rgba(99, 102, 241, 0.3)';
            bgStyle = 'background: rgba(99, 102, 241, 0.08); backdrop-filter: blur(10px);';
            pulseAnim = 'animation: cs-pulse-slow 3s infinite;';
        } else if (status === 'error') {
            dotColor = '#ef4444'; // Red
            borderColor = 'rgba(239, 68, 68, 0.3)';
            bgStyle = 'background: rgba(239, 68, 68, 0.1); backdrop-filter: blur(10px);';
            pulseAnim = 'animation: cs-pulse 1s infinite;';
        } else if (status === 'offline') {
            dotColor = '#64748b'; // Gray
            borderColor = 'rgba(100, 116, 139, 0.2)';
            bgStyle = 'background: rgba(100, 116, 139, 0.05); backdrop-filter: blur(10px);';
        }

        const badgeHtml = `
            <div id="cloud-sync-badge" class="sync-badge" style="border:1px solid ${borderColor}; ${bgStyle}">
                <div class="sync-badge-dot" style="background:${dotColor}; box-shadow:0 0 8px ${dotColor}; ${pulseAnim}"></div>
                <span class="sync-badge-text" style="color:${dotColor};">${label}</span>
            </div>
            <style>
                @keyframes cs-pulse {
                    0% { opacity: 1; transform: scale(1); }
                    50% { opacity: 0.4; transform: scale(1.25); }
                    100% { opacity: 1; transform: scale(1); }
                }
                @keyframes cs-pulse-slow {
                    0% { opacity: 1; transform: scale(1); }
                    50% { opacity: 0.6; transform: scale(1.1); }
                    100% { opacity: 1; transform: scale(1); }
                }
            </style>
        `;
        
        // Remove existing badge if present
        const existing = document.getElementById('cloud-sync-badge');
        if (existing) existing.remove();

        // Insert inside topbar-user at the start, or fallback to header end
        if (topbarUser) {
            topbarUser.insertAdjacentHTML('afterbegin', badgeHtml);
        } else if (header) {
            header.insertAdjacentHTML('beforeend', badgeHtml);
        }
    },

    /**
     * Subscribes to the Appwrite Realtime API for incoming dispatches.
     * @returns {void}
     */
    subscribeToDispatches: () => {
        if (!CloudSync.client || !CloudSync.config) return;

        const channel = `databases.${CloudSync.config.databaseId}.collections.${CloudSync.config.ordersCollectionId}.documents`;
        console.log(`📡 Subscribed to Appwrite Realtime: ${channel}`);
        
        try {
            CloudSync.unsubscribe = CloudSync.client.subscribe(channel, response => {
                console.log("⚡ Appwrite Realtime Event Received:", response);
                
                const syncedOrder = response.payload;
                if (!syncedOrder || !syncedOrder.id) return;
                
                const localOrder = getLocalOrder(syncedOrder.id);
                
                const hasChanged = !localOrder || 
                                   localOrder.status !== syncedOrder.status ||
                                   localOrder.kg !== syncedOrder.kg ||
                                   localOrder.actualKg !== syncedOrder.actualKg ||
                                   localOrder.quality !== syncedOrder.quality ||
                                   localOrder.riderId !== syncedOrder.riderId ||
                                   localOrder.riderName !== syncedOrder.riderName;
                                   
                if (hasChanged) {
                    console.log(`🔄 Synced order [${syncedOrder.id}] has changes. Saving locally.`);
                    
                    const originalLive = CloudSync.isLive;
                    CloudSync.isLive = false;
                    try {
                        window.saveOrder(syncedOrder);
                    } finally {
                        CloudSync.isLive = originalLive;
                    }
                    
                    if (window.showToast) {
                        window.showToast("☁️ Real-Time: Dispatch status updated!");
                    }
                    
                    if (window.refreshCurrentView) {
                        window.refreshCurrentView(true);
                    }
                }
            });
        } catch (e) {
            console.error("Failed to subscribe to Appwrite Realtime:", e);
            CloudSync.renderSyncBadge('error', 'Sync Error');
        }
    },

    /**
     * Sanitizes an order object to match database attribute schemas.
     * Ensures all values match correct types.
     * @param {Object} doc - Raw order document.
     * @returns {Object} Sanitized object ready for Appwrite.
     */
    sanitizeDoc: (doc) => {
        const sanitized = {};
        
        const stringFields = ['id', 'providerId', 'providerOrg', 'wasteType', 'shift', 'plantId', 'plantName', 'status', 'riderId', 'riderName', 'quality'];
        stringFields.forEach(field => {
            if (doc[field] !== undefined && doc[field] !== null) {
                sanitized[field] = String(doc[field]);
            } else {
                sanitized[field] = '';
            }
        });

        const numberFields = ['ts', 'providerLat', 'providerLng', 'kg', 'actualKg'];
        numberFields.forEach(field => {
            if (doc[field] !== undefined && doc[field] !== null) {
                sanitized[field] = Number(doc[field]);
            } else {
                sanitized[field] = 0;
            }
        });

        return sanitized;
    },

    /**
     * Pushes a local state change to the Appwrite Database.
     * Performs timestamp-based conflict resolution before overwriting:
     * if the cloud version is newer, the stale local write is discarded
     * and local storage is updated with the cloud version instead.
     * @param {string} collection - Target collection ID.
     * @param {Object} payload - Data to sync.
     * @returns {Promise<void>}
     */
    pushDocument: async (collection, payload) => {
        if (!CloudSync.isLive || !CloudSync.databases || !CloudSync.config) return;
        
        CloudSync.renderSyncBadge('syncing', 'Syncing...');

        try {
            const sanitizedDoc = CloudSync.sanitizeDoc(payload);
            const { databaseId, ordersCollectionId } = CloudSync.config;

            // Timestamp-based conflict resolution: fetch cloud version first
            let cloudDoc = null;
            try {
                cloudDoc = await CloudSync.databases.getDocument(
                    databaseId,
                    ordersCollectionId,
                    payload.id
                );
            } catch (fetchErr) {
                // 404 means document does not exist on cloud yet — proceed to create
                if (fetchErr.code !== 404) {
                    console.warn('[CloudSync] Failed to fetch cloud doc for conflict check:', fetchErr);
                }
            }

            if (cloudDoc) {
                // Use resolveConflict: build local/server representations
                const localAction = { id: payload.id, timestamp: payload.ts || 0 };
                const serverData = { id: cloudDoc.id || cloudDoc.$id, timestamp: cloudDoc.ts || 0 };
                const resolution = resolveConflict(localAction, serverData);

                if (resolution === null) {
                    // Cloud is newer or duplicate — discard stale local write, update local cache
                    console.log(`[CloudSync] Cloud version is newer for ${payload.id} — discarding local overwrite.`);
                    const localKey = STORAGE_KEY_PREFIX + 'ord:' + payload.id;
                    try {
                        localStorage.setItem(localKey, JSON.stringify(cloudDoc));
                    } catch { /* ignore storage errors */ }
                    if (window.refreshCurrentView) window.refreshCurrentView(false);
                    CloudSync.renderSyncBadge('live', 'Cloud Live');
                    return;
                }
            }

            // Local is newer or no cloud version exists — proceed with upsert
            try {
                await CloudSync.databases.updateDocument(
                    databaseId,
                    ordersCollectionId,
                    payload.id,
                    sanitizedDoc
                );
                console.log(`☁️ Synced to Appwrite (Updated) -> Collection [${ordersCollectionId}]`, sanitizedDoc);
            } catch (updateErr) {
                if (updateErr.code === 404) {
                    await CloudSync.databases.createDocument(
                        databaseId,
                        ordersCollectionId,
                        payload.id,
                        sanitizedDoc
                    );
                    console.log(`☁️ Synced to Appwrite (Created) -> Collection [${ordersCollectionId}]`, sanitizedDoc);
                } else {
                    throw updateErr;
                }
            }

            CloudSync.renderSyncBadge('live', 'Cloud Live');
        } catch (e) {
            console.error("Failed to sync document to Appwrite:", e);
            CloudSync.renderSyncBadge('error', 'Sync Error');
        }
    },

    /**
     * Sanitizes an account object for Appwrite storage.
     * @param {Object} account - Raw account object.
     * @returns {Object} Sanitized object.
     */
    sanitizeAccount: (account) => {
        const sanitized = {};
        ['id', 'role', 'name', 'org'].forEach(f => {
            sanitized[f] = account[f] != null ? String(account[f]) : '';
        });
        ['lat', 'lng', 'tokens', 'staked'].forEach(f => {
            sanitized[f] = account[f] != null ? Number(account[f]) : 0;
        });
        return sanitized;
    },

    /**
     * Upserts an account document to Appwrite.
     * @param {Object} account - Account object with at minimum an `id` field.
     * @returns {Promise<void>}
     */
    pushAccount: async (account) => {
        if (!CloudSync.isLive || !CloudSync.databases || !CloudSync.config?.accountsCollectionId) return;
        try {
            const sanitized = CloudSync.sanitizeAccount(account);
            const { databaseId, accountsCollectionId } = CloudSync.config;
            try {
                await CloudSync.databases.updateDocument(databaseId, accountsCollectionId, account.id, sanitized);
            } catch (e) {
                if (e.code === 404) {
                    await CloudSync.databases.createDocument(databaseId, accountsCollectionId, account.id, sanitized);
                } else throw e;
            }
        } catch (e) {
            console.error('[CloudSync] pushAccount failed:', e);
            CloudSync.queueOfflineWrite('acc:' + account.id, account);
        }
    },

    /**
     * Fetches a single account document from Appwrite by UID.
     * @param {string} uid - Account ID.
     * @returns {Promise<Object|null>}
     */
    fetchAccount: async (uid) => {
        if (!CloudSync.isLive || !CloudSync.databases || !CloudSync.config?.accountsCollectionId) return null;
        try {
            return await CloudSync.databases.getDocument(
                CloudSync.config.databaseId,
                CloudSync.config.accountsCollectionId,
                uid
            );
        } catch (e) {
            return null;
        }
    },

    /**
     * Fetches all orders related to a given user UID from Appwrite.
     * @param {string} uid - User ID (matched against providerId, riderId, or plantId).
     * @returns {Promise<Array>}
     */
    fetchOrdersForUser: async (uid) => {
        if (!CloudSync.isLive || !CloudSync.databases || !CloudSync.config) return [];
        try {
            const { Query } = window.Appwrite;
            const [providerRes, riderRes, plantRes] = await Promise.allSettled([
                CloudSync.databases.listDocuments(
                    CloudSync.config.databaseId,
                    CloudSync.config.ordersCollectionId,
                    [Query.equal('providerId', uid)]
                ),
                CloudSync.databases.listDocuments(
                    CloudSync.config.databaseId,
                    CloudSync.config.ordersCollectionId,
                    [Query.equal('riderId', uid)]
                ),
                CloudSync.databases.listDocuments(
                    CloudSync.config.databaseId,
                    CloudSync.config.ordersCollectionId,
                    [Query.equal('plantId', uid)]
                )
            ]);
            const docs = [];
            [providerRes, riderRes, plantRes].forEach(r => {
                if (r.status === 'fulfilled') docs.push(...(r.value.documents || []));
            });
            // Deduplicate by order id
            return [...new Map(docs.map(d => [d.id, d])).values()];
        } catch (e) {
            console.error('[CloudSync] fetchOrdersForUser failed:', e);
            return [];
        }
    },

    /**
     * Queues a write for offline retry.
     * Primary storage: IndexedDB via offline-sync.js.
     * Fallback: localStorage under a dedicated key if IndexedDB is unavailable.
     * Latest value for any given key wins (deduplication).
     * @param {string} key - Data key (e.g. 'ord:abc123').
     * @param {Object} data - Data payload.
     */
    queueOfflineWrite: (key, data) => {
        // Determine the action type from the key prefix
        const type = key.startsWith('ord:') ? 'sync-order' :
                     key.startsWith('acc:') ? 'sync-account' : 'dispatch';

        // Try IndexedDB first
        if (isDBReady()) {
            idbQueueAction(type, data).then(() => {
                console.log(`[CloudSync] Queued offline write to IndexedDB for key: ${key}`);
            }).catch((err) => {
                console.warn('[CloudSync] IndexedDB queue failed, falling back to localStorage:', err);
                CloudSync._fallbackLocalStorageQueue(key, data);
            });
        } else {
            CloudSync._fallbackLocalStorageQueue(key, data);
        }
    },

    /**
     * Fallback: queue to localStorage if IndexedDB is unavailable.
     * @param {string} key
     * @param {Object} data
     */
    _fallbackLocalStorageQueue: (key, data) => {
        try {
            const queue = JSON.parse(localStorage.getItem('regenx-offline-queue') || '[]');
            const filtered = queue.filter(item => item.key !== key);
            filtered.push({ key, data, ts: Date.now() });
            localStorage.setItem('regenx-offline-queue', JSON.stringify(filtered));
            console.log(`[CloudSync] Queued offline write to localStorage for key: ${key}`);
        } catch (e) {
            console.warn('[CloudSync] Failed to queue offline write:', e);
        }
    },

    /**
     * Flushes all offline-queued writes to Appwrite.
     * Drains both the IndexedDB queue (via offline-sync.js syncPendingActions)
     * and any legacy localStorage entries.
     * Called when the app comes back online.
     * @returns {Promise<void>}
     */
    flushOfflineQueue: async () => {
        if (!CloudSync.isLive) return;

        // Flush legacy localStorage queue entries
        try {
            const queue = JSON.parse(localStorage.getItem('regenx-offline-queue') || '[]');
            if (queue.length > 0) {
                console.log(`[CloudSync] Flushing ${queue.length} legacy localStorage queued writes...`);
                const failed = [];
                for (const item of queue) {
                    try {
                        if (item.key.startsWith('ord:') && item.data?.id) {
                            await CloudSync.pushDocument(CloudSync.config.ordersCollectionId, item.data);
                        } else if (item.key.startsWith('acc:') && item.data?.id) {
                            await CloudSync.pushAccount(item.data);
                        }
                    } catch (e) {
                        failed.push(item);
                    }
                }
                localStorage.setItem('regenx-offline-queue', JSON.stringify(failed));
                if (failed.length === 0) {
                    window.showToast?.('✅ All offline data synced to cloud!');
                } else {
                    console.warn(`[CloudSync] ${failed.length} legacy writes still pending after flush.`);
                }
            }
        } catch (e) {
            console.error('[CloudSync] flushOfflineQueue (localStorage) failed:', e);
        }
    },

    /**
     * Hydrates localStorage from Appwrite on login.
     * Cloud data wins for account fields (tokens, staked).
     * Cloud data wins for orders where cloud timestamp is newer.
     * @param {string} uid - The logged-in user's ID.
     * @returns {Promise<void>}
     */
    hydrateFromCloud: async (uid) => {
        if (!CloudSync.isLive) return;
        CloudSync.renderSyncBadge('syncing', 'Hydrating...');
        try {
            const [cloudAccount, cloudOrders] = await Promise.all([
                CloudSync.fetchAccount(uid),
                CloudSync.fetchOrdersForUser(uid)
            ]);

            // Hydrate account — cloud wins on financial fields
            if (cloudAccount) {
                const localRaw = localStorage.getItem(STORAGE_KEY_PREFIX + 'acc:' + uid);
                const localAcc = localRaw ? JSON.parse(localRaw) : {};
                const merged = { ...localAcc, ...cloudAccount };
                localStorage.setItem(STORAGE_KEY_PREFIX + 'acc:' + uid, JSON.stringify(merged));
                if (window.SESSION?.id === uid) {
                    Object.assign(window.SESSION, merged);
                    // Refresh token display if visible
                    const tokenEl = document.getElementById('token-balance');
                    if (tokenEl) tokenEl.textContent = merged.tokens ?? 0;
                }
            }

            // Hydrate orders — cloud wins if timestamp is newer
            for (const order of cloudOrders) {
                const localKey = STORAGE_KEY_PREFIX + 'ord:' + order.id;
                const localRaw = localStorage.getItem(localKey);
                const localOrder = localRaw ? JSON.parse(localRaw) : null;
                if (!localOrder || (order.ts > (localOrder.ts || 0))) {
                    localStorage.setItem(localKey, JSON.stringify(order));
                }
            }

            CloudSync.renderSyncBadge('live', 'Cloud Live');
            console.log(`☁️ Hydrated from cloud: account + ${cloudOrders.length} orders`);

            // Refresh the current dashboard view with fresh data
            window.refreshCurrentView?.(true);
        } catch (e) {
            console.error('[CloudSync] hydrateFromCloud failed:', e);
            CloudSync.renderSyncBadge('error', 'Sync Error');
        }
    }
};

window.CloudSync = CloudSync;
// Phase 2 Task 4: Local-first IndexedDB background sync active
