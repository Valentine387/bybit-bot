// AlgoRhythm PWA Service Worker
// Handles: offline caching, push notifications, background sync

const CACHE_NAME = 'algorhythm-v1';
const ASSETS_TO_CACHE = [
  '/bybit-bot/',
  '/bybit-bot/index.html',
  '/bybit-bot/manifest.json',
  '/bybit-bot/icon-192.png',
  '/bybit-bot/icon-512.png',
];

// ── Install: cache core assets ────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(ASSETS_TO_CACHE).catch(err => {
        console.log('Cache install partial:', err);
      });
    })
  );
  self.skipWaiting();
});

// ── Activate: clean old caches ────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

// ── Fetch: serve from cache, fallback to network ──────────────────────
self.addEventListener('fetch', event => {
  // Only cache GET requests for our own assets
  if (event.request.method !== 'GET') return;
  if (event.request.url.includes('api.bybit.com')) return;
  if (event.request.url.includes('onrender.com')) return;
  if (event.request.url.includes('tradingview.com')) return;

  event.respondWith(
    caches.match(event.request).then(cached => {
      if (cached) return cached;
      return fetch(event.request).then(response => {
        // Cache successful responses for our assets
        if (response.ok && event.request.url.includes('/bybit-bot/')) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      }).catch(() => {
        // Offline fallback — return cached index.html
        return caches.match('/bybit-bot/index.html');
      });
    })
  );
});

// ── Push Notifications ────────────────────────────────────────────────
self.addEventListener('push', event => {
  let data = { title: 'AlgoRhythm', body: 'New trading signal', type: 'info' };
  try {
    data = event.data ? event.data.json() : data;
  } catch(e) {
    data.body = event.data ? event.data.text() : data.body;
  }

  const type = data.type || 'info';

  // Icon and badge based on notification type
  const icons = {
    buy:    { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'trade-buy'    },
    sell:   { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'trade-sell'   },
    tp:     { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'trade-tp'     },
    sl:     { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'trade-sl'     },
    signal: { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'signal'       },
    regime: { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'regime'       },
    info:   { icon: 'icon-192.png', badge: 'icon-192.png', tag: 'info'         },
  };

  const cfg = icons[type] || icons.info;

  const options = {
    body:    data.body,
    icon:    cfg.icon,
    badge:   cfg.badge,
    tag:     data.tag || cfg.tag,
    data:    { url: data.url || '/bybit-bot/', type },
    vibrate: type === 'sl' ? [300, 100, 300] : type === 'tp' ? [100, 50, 100] : [100],
    requireInteraction: ['sl','tp','buy'].includes(type),
    actions: data.actions || [],
    timestamp: Date.now(),
  };

  event.waitUntil(
    self.registration.showNotification(data.title || 'AlgoRhythm', options)
  );
});

// ── Notification Click ────────────────────────────────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();

  const url = event.notification.data?.url || '/bybit-bot/';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clientList => {
      // If app already open, focus it
      for (const client of clientList) {
        if (client.url.includes('/bybit-bot/') && 'focus' in client) {
          return client.focus();
        }
      }
      // Otherwise open new window
      if (clients.openWindow) {
        return clients.openWindow(url);
      }
    })
  );
});

// ── Background Sync (for when connection resumes) ─────────────────────
self.addEventListener('sync', event => {
  if (event.tag === 'sync-positions') {
    // Triggered when network comes back — app will refresh positions
    self.clients.matchAll().then(clients => {
      clients.forEach(client => client.postMessage({ type: 'SYNC_POSITIONS' }));
    });
  }
});

// ── Message from app ──────────────────────────────────────────────────
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
