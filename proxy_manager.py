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


def get_proxy_list() -> list[dict]:
    """Return the ordered list of configured proxies."""
    return _PROXY_LIST


def _test_proxy(proxy: dict, test_url: str = "https://www.reddit.com/", timeout: int = 8) -> bool:
    """
    Returns True if *proxy* can reach *test_url* successfully.
    """
    proxies = {"http": proxy["url"], "https": proxy["url"]}
    try:
        resp = requests.get(
            test_url,
            proxies=proxies,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout,
        )
        return resp.status_code == 200
    except Exception as exc:
        logger.debug(f"  {proxy['label']} test failed: {exc}")
        return False


def get_available_proxy(test_url: str = "https://www.reddit.com/") -> dict | None:
    """
    Iterate through the proxy list in order and return the first one that is
    reachable.  Returns None if no proxy is available (or none are configured).
    """
    if not _PROXY_LIST:
        logger.warning("⚠️  No proxies configured (PROXY_HOST not set).")
        return None

    for proxy in _PROXY_LIST:
        logger.info(f"🔍 Testing {proxy['label']} …")
        if _test_proxy(proxy, test_url):
            logger.info(f"✅ {proxy['label']} is ONLINE.")
            return proxy
        else:
            logger.warning(f"🔴 {proxy['label']} is OFFLINE, trying next…")

    logger.error("❌ All proxies are OFFLINE.")
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