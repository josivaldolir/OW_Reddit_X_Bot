import os
import logging
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Build the proxy list from environment variables.
#
# Proxy 1: PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS
# Proxy 2: PROXY_HOST_2, PROXY_PORT_2, PROXY_USER_2, PROXY_PASS_2
# (Extend with PROXY_HOST_3 … if you ever need more.)
# ---------------------------------------------------------------------------

def _build_proxy_list() -> list[dict]:
    """
    Returns a list of proxy config dicts, ordered by priority (index 0 = first try).
    Each dict:
        {
            "label":  "Proxy 1",
            "url":    "http://user:pass@host:port",   # ready-to-use URL
        }
    Only proxies whose PROXY_HOST_n is set are included.
    """
    slots = [
        {
            "label": "Proxy 1",
            "host":  os.getenv("PROXY_HOST", ""),
            "port":  os.getenv("PROXY_PORT", "8080"),
            "user":  os.getenv("PROXY_USER", ""),
            "pass":  os.getenv("PROXY_PASS", ""),
        },
        {
            "label": "Proxy 2",
            "host":  os.getenv("PROXY_HOST_2", ""),
            "port":  os.getenv("PROXY_PORT_2", "8080"),
            "user":  os.getenv("PROXY_USER_2", ""),
            "pass":  os.getenv("PROXY_PASS_2", ""),
        },
    ]

    proxies = []
    for slot in slots:
        if not slot["host"]:
            continue

        if slot["user"] and slot["pass"]:
            url = f"http://{slot['user']}:{slot['pass']}@{slot['host']}:{slot['port']}"
        else:
            url = f"http://{slot['host']}:{slot['port']}"

        proxies.append({"label": slot["label"], "url": url})

    return proxies


# Module-level cache so we don't rebuild on every call
_PROXY_LIST: list[dict] = _build_proxy_list()

# ---------------------------------------------------------------------------
# Run-level cache — proxy is tested AT MOST ONCE per process lifetime.
# Sentinel value _UNSET means "not yet resolved".
# ---------------------------------------------------------------------------
_UNSET = object()
_cached_proxy: object = _UNSET   # will hold dict | None after first resolution


def get_proxy_list() -> list[dict]:
    """Return the ordered list of configured proxies."""
    return _PROXY_LIST


def _test_proxy(proxy: dict, timeout: int = 10) -> bool:
    """
    Returns True if the proxy can reach Reddit successfully.

    Uses the lightweight JSON endpoint (same URL confirmed working in manual
    tests) instead of the HTML homepage, which can return non-200 redirects
    or consent pages even when the proxy itself is perfectly healthy.
    """
    # .json endpoint is lightweight, auth-free, and returns a clean 200
    test_url = "https://www.reddit.com/r/test.json?limit=1"
    proxies = {"http": proxy["url"], "https": proxy["url"]}
    try:
        resp = requests.get(
            test_url,
            proxies=proxies,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=timeout,
        )
        if resp.status_code == 200:
            return True
        # Log the real status so we know WHY it failed (429, 403, 503 …)
        logger.warning(f"  {proxy['label']} test got HTTP {resp.status_code}")
        return False
    except requests.exceptions.ProxyError as exc:
        logger.warning(f"  {proxy['label']} proxy error: {exc}")
        return False
    except requests.exceptions.Timeout:
        logger.warning(f"  {proxy['label']} timed out after {timeout}s")
        return False
    except Exception as exc:
        logger.warning(f"  {proxy['label']} test failed: {exc}")
        return False


def get_available_proxy() -> dict | None:
    """
    Return the first reachable proxy, or None if all are offline.

    Result is cached for the entire process lifetime — subsequent calls
    return the cached value immediately without making any new HTTP requests.
    This prevents rapid-fire proxy checks that can trigger 429s on CCProxy.
    """
    global _cached_proxy

    if _cached_proxy is not _UNSET:
        # Already resolved this run — return instantly, no HTTP call
        return _cached_proxy  # type: ignore[return-value]

    if not _PROXY_LIST:
        logger.warning("⚠️  No proxies configured (PROXY_HOST not set).")
        _cached_proxy = None
        return None

    for proxy in _PROXY_LIST:
        logger.info(f"🔍 Testing {proxy['label']} …")
        if _test_proxy(proxy):
            logger.info(f"✅ {proxy['label']} is ONLINE – result cached for this run.")
            _cached_proxy = proxy
            return proxy
        else:
            logger.warning(f"🔴 {proxy['label']} is OFFLINE, trying next…")

    logger.error("❌ All proxies are OFFLINE.")
    _cached_proxy = None
    return None


def get_requests_proxies(proxy: dict | None) -> dict | None:
    """
    Convert a proxy dict (as returned by get_available_proxy) into the
    *proxies* kwarg format expected by the `requests` library.
    Returns None if proxy is None (caller should use direct connection).
    """
    if proxy is None:
        return None
    return {"http": proxy["url"], "https": proxy["url"]}


def is_any_proxy_available() -> bool:
    """Convenience wrapper – returns True if at least one proxy is reachable."""
    return get_available_proxy() is not None