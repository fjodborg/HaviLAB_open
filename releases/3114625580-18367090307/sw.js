/* HaviLAB service worker.
 *
 * Cache ownership is intentionally narrow:
 * - navigation requests are network-first with a cached shell fallback;
 * - versioned same-origin static assets are cache-first;
 * - update manifests and requests marked no-store are never intercepted.
 *
 * A new worker waits instead of skipping activation, so an open annotation
 * session is not replaced underneath the user. The release build inserts a
 * unique build id into CACHE_NAME before deployment.
 */

const SCOPE_PATH = self.location.pathname.replace(/\/sw\.js$/, "/");
const CACHE_PREFIX = `hvat-${SCOPE_PATH.replace(/[^A-Za-z0-9]+/g, "-")}-`;
const CACHE_NAME = `${CACHE_PREFIX}8d813cdfb480648b41ca3a95a621cfd2f81699fd-3114625580-18367090307`;
const SHELL = ["./", "./index.html", "./manifest.json"];
const ASSET_EXTENSIONS = [
    ".css",
    ".js",
    ".json",
    ".wasm",
    ".png",
    ".svg",
    ".ico",
    ".woff",
    ".woff2",
];

self.addEventListener("install", (event) => {
    event.waitUntil(
        caches
            .open(CACHE_NAME)
            .then((cache) => cache.addAll(SHELL))
            .then(() => {
                // Do not call skipWaiting here. A new worker waits until the
                // existing clients are closed or explicitly ask to activate.
            })
    );
});

self.addEventListener("activate", (event) => {
    event.waitUntil(
        caches
            .keys()
            .then((keys) =>
                Promise.all(
                    keys
                        .filter(
                            (key) => key.startsWith(CACHE_PREFIX) && key !== CACHE_NAME
                        )
                        .map((key) => caches.delete(key))
                )
            )
            .then(() => {
                // Do not clients.claim(): old pages may still reference the
                // previous build's hashed assets.
            })
    );
});

self.addEventListener("message", (event) => {
    if (event.data && event.data.type === "SKIP_WAITING") {
        self.skipWaiting();
    }
});

self.addEventListener("fetch", (event) => {
    const request = event.request;
    if (request.method !== "GET") {
        return;
    }

    const url = new URL(request.url);
    if (url.origin !== self.location.origin) {
        return;
    }

    // Never serve update discovery or the worker itself from the app cache.
    if (
        url.pathname.endsWith("/sw.js") ||
        url.pathname.endsWith("/release-manifest.json") ||
        url.pathname.endsWith("/release-manifest.json.sig") ||
        request.cache === "no-store"
    ) {
        return;
    }

    if (request.mode === "navigate") {
        event.respondWith(networkFirst(request));
        return;
    }

    if (isCacheableAsset(url.pathname)) {
        event.respondWith(cacheFirst(request));
    }
});

async function networkFirst(request) {
    const cache = await caches.open(CACHE_NAME);
    try {
        const response = await fetch(request);
        if (response.ok) {
            await cache.put(request, response.clone());
        }
        return response;
    } catch (error) {
        const cached = await cache.match(request);
        if (cached) {
            return cached;
        }
        const shell = await cache.match(new URL("index.html", self.registration.scope));
        if (shell) {
            return shell;
        }
        throw error;
    }
}

async function cacheFirst(request) {
    const cache = await caches.open(CACHE_NAME);
    const cached = await cache.match(request);
    if (cached) {
        return cached;
    }
    const response = await fetch(request);
    if (response.ok) {
        await cache.put(request, response.clone());
    }
    return response;
}

function isCacheableAsset(pathname) {
    if (pathname.startsWith("/api/")) {
        return false;
    }
    return ASSET_EXTENSIONS.some((extension) => pathname.endsWith(extension));
}
