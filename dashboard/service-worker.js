// Minimal service worker for PWA installability.
//
// Strategy: network-first for everything (the dashboard is a live data
// surface — caching stale data would mislead). The SW exists primarily
// to satisfy installability requirements so Chrome / Safari treat the
// page as a real PWA. We do cache the app shell (admin.html + icons +
// manifest) so the splash + first paint still appears when offline,
// then network errors fail naturally for data calls.

const CACHE_NAME = "lead-audit-shell-v1";
const SHELL_FILES = [
  "admin.html",
  "manifest.json",
  "icon-192.png",
  "icon-512.png",
  "apple-touch-icon.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  // Drop any older shell caches on activation.
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  // Only intercept same-origin GETs — Supabase / GHL calls go through cleanly.
  if (req.method !== "GET" || new URL(req.url).origin !== self.location.origin) return;
  // Network-first; fall back to cached shell if offline so the app at
  // least shows its own UI rather than the browser's offline page.
  event.respondWith(
    fetch(req)
      .then((resp) => {
        // Refresh cached shell file when we successfully fetch one.
        const url = new URL(req.url);
        const file = url.pathname.split("/").pop();
        if (SHELL_FILES.includes(file)) {
          const copy = resp.clone();
          caches.open(CACHE_NAME).then((c) => c.put(req, copy));
        }
        return resp;
      })
      .catch(() => caches.match(req).then((hit) => hit || caches.match("admin.html")))
  );
});
