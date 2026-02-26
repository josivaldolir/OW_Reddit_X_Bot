import requests
import logging
import os
import re
from random import choice
from bs4 import BeautifulSoup
from queue_manager import (
    initialize_queue_db,
    add_json_batch,
    get_next_unposted_post,
    get_queue_stats,
    MAX_JSON_BATCHES
)
from proxy_manager import get_available_proxy, get_requests_proxies

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_proxy_available() -> bool:
    """
    Returns True if at least one configured proxy is reachable.
    Replaces the old single-proxy check so callers don't need to change.
    """
    proxy = get_available_proxy()
    return proxy is not None


def extract_post_id_from_url(url):
    """Extrai o ID do post da URL do Reddit"""
    match = re.search(r'/comments/([a-z0-9]+)/', url)
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def parse_reddit_html(html_content):
    soup = BeautifulSoup(html_content, 'lxml')
    posts = []
    filtered_count = 0

    post_elements = soup.find_all('shreddit-post')

    if not post_elements:
        post_elements = soup.find_all('div', {'data-context': 'listing'})

    if not post_elements:
        post_elements = soup.find_all('div', class_=lambda x: x and 'thing' in x)

    logger.info(f"🔍 Encontrados {len(post_elements)} elementos de post no HTML")

    for post_elem in post_elements:
        try:
            post_info = extract_post_data(post_elem, soup)
            if post_info and post_info.get('id'):
                posts.append(post_info)
            elif post_info is None:
                filtered_count += 1
        except Exception as e:
            logger.debug(f"Erro ao processar post: {e}")
            continue

    if filtered_count > 0:
        logger.info(f"🚫 {filtered_count} posts filtrados (anúncios/promocionais)")

    return posts


def extract_post_data(post_elem, soup):
    post_info = {
        "id": '',
        "title": '',
        "content": '',
        "url": '',
        "s_img": '',
        "m_img": [],
        "video": '',
        "video_fallback_url": ''
    }

    def fix_url(url):
        if not url:
            return ''
        if url.startswith('//'):
            return 'https:' + url
        if not url.startswith('http'):
            return 'https://' + url
        return url

    def get_high_res_image_url(preview_url):
        if not preview_url:
            return ''
        if '?' in preview_url:
            base_url = preview_url.split('?')[0]
        else:
            base_url = preview_url
        if 'preview.redd.it' in base_url:
            high_res_url = base_url.replace('preview.redd.it', 'i.redd.it')
            logger.debug(f"Converted to high-res: {high_res_url}")
            return high_res_url
        return base_url

    def extract_gallery_images(post_elem):
        seen = set()
        images = []

        for link in post_elem.find_all('a', href=True):
            href = link.get('href', '')
            if 'redd.it' not in href:
                continue
            if not any(ext in href.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                continue
            high_res = get_high_res_image_url(fix_url(href.replace('&amp;', '&')))
            if high_res and high_res not in seen:
                seen.add(high_res)
                images.append(high_res)

        if not images:
            for img in post_elem.find_all('img'):
                src = img.get('src', '')
                if 'redd.it' not in src:
                    continue
                width = img.get('width', '9999')
                try:
                    if int(width) < 300:
                        continue
                except (ValueError, TypeError):
                    pass
                high_res = get_high_res_image_url(fix_url(src.replace('&amp;', '&')))
                if high_res and high_res not in seen:
                    seen.add(high_res)
                    images.append(high_res)

        return images[:4]

    is_new_reddit = post_elem.name == 'shreddit-post'

    if is_new_reddit:
        post_info['id'] = post_elem.get('id', '').replace('t3_', '')
        post_info['title'] = post_elem.get('post-title', '')
        permalink = post_elem.get('permalink', '')
        if permalink:
            post_info['url'] = f"https://www.reddit.com{permalink}"
        content_html = post_elem.get('content-href', '')
        if content_html:
            post_info['content'] = content_html[:500]
        thumbnail = post_elem.get('thumbnail', '')
        if thumbnail and 'redd.it' in thumbnail:
            high_res = get_high_res_image_url(thumbnail)
            post_info['s_img'] = fix_url(high_res)
        gallery_images = extract_gallery_images(post_elem)
        if len(gallery_images) > 1:
            post_info['m_img'] = gallery_images
            post_info['s_img'] = ''
        elif len(gallery_images) == 1 and not post_info['s_img']:
            post_info['s_img'] = gallery_images[0]
        if post_elem.get('is-video') == 'true':
            post_info['video'] = post_info['url']
            post_info['s_img'] = ''
            post_info['m_img'] = []
    else:
        post_id = post_elem.get('data-fullname', '').replace('t3_', '')
        if not post_id:
            post_id = post_elem.get('id', '').replace('thing_t3_', '')
        post_info['id'] = post_id
        title_elem = post_elem.find('a', class_='title')
        if not title_elem:
            title_elem = post_elem.find('p', class_='title')
        if title_elem:
            post_info['title'] = title_elem.get_text(strip=True)
        permalink = post_elem.get('data-permalink', '')
        if permalink:
            post_info['url'] = f"https://www.reddit.com{permalink}"
        elif title_elem and title_elem.get('href'):
            href = title_elem.get('href')
            if href.startswith('/r/'):
                post_info['url'] = f"https://www.reddit.com{href}"
            else:
                post_info['url'] = href
        expando = post_elem.find('div', class_='expando')
        if expando:
            usertext = expando.find('div', class_='usertext-body')
            if usertext:
                post_info['content'] = usertext.get_text(strip=True)[:500]
        thumbnail = post_elem.get('data-thumbnail', '')
        if thumbnail and 'redd.it' in thumbnail and thumbnail not in ['self', 'default', 'nsfw', 'spoiler']:
            high_res = get_high_res_image_url(thumbnail)
            post_info['s_img'] = fix_url(high_res)
        if not post_info['s_img']:
            preview = post_elem.find('a', class_='thumbnail')
            if preview:
                img = preview.find('img')
                if img and img.get('src'):
                    src = img.get('src')
                    if 'redd.it' in src:
                        high_res = get_high_res_image_url(src)
                        post_info['s_img'] = fix_url(high_res)
        gallery_images = extract_gallery_images(post_elem)
        if len(gallery_images) > 1:
            post_info['m_img'] = gallery_images
            post_info['s_img'] = ''
        elif len(gallery_images) == 1 and not post_info['s_img']:
            post_info['s_img'] = gallery_images[0]
        domain = post_elem.get('data-domain', '')
        is_video = post_elem.get('data-is-video', 'false') == 'true'
        if is_video or domain == 'v.redd.it':
            post_info['video'] = post_info['url']
            post_info['s_img'] = ''
            post_info['m_img'] = []

    if post_elem.get('data-stickied') == 'true' or post_elem.get('stickied') == 'true':
        return None

    if post_info['url']:
        if '/user/' in post_info['url'] or '/u/' in post_info['url']:
            logger.debug(f"⚠️ Post promocional ignorado: {post_info['url']}")
            return None
        allowed_subreddits = ['Overwatch', 'Overwatch_Memes']
        is_from_allowed = any(f'/r/{sub}/' in post_info['url'] for sub in allowed_subreddits)
        if not is_from_allowed:
            logger.debug(f"⚠️ Post de outro subreddit ignorado: {post_info['url']}")
            return None

    if not post_info['id'] or not post_info['title']:
        return None

    if post_info['s_img']:
        logger.debug(f"Post {post_info['id']}: Imagem única encontrada")
    if post_info['m_img']:
        logger.debug(f"Post {post_info['id']}: Galeria com {len(post_info['m_img'])} imagens")
    if post_info['video']:
        logger.debug(f"Post {post_info['id']}: Vídeo encontrado")

    return post_info


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_posts_from_reddit_html(subreddit, limit=50):
    """
    Busca posts do Reddit via HTML scraping.

    Proxy selection is delegated to proxy_manager: tries Proxy 1 first,
    falls back to Proxy 2 (if configured), then falls back to a direct
    connection as a last resort.
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/131.0.0.0 Safari/537.36'
        ),
        'Accept': (
            'text/html,application/xhtml+xml,application/xml;'
            'q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8'
        ),
        'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Cache-Control': 'max-age=0',
    })

    # --- Proxy selection with fallback ---
    active_proxy = get_available_proxy()
    if active_proxy:
        proxy_dict = get_requests_proxies(active_proxy)
        session.proxies.update(proxy_dict)
        logger.info(f"🔐 Using {active_proxy['label']} for Reddit scraping")
    else:
        logger.warning("⚠️  All proxies offline – attempting direct connection")

    # Warm up cookies
    try:
        logger.info("🍪 Obtaining cookies from homepage…")
        home_resp = session.get("https://old.reddit.com/", timeout=10)
        if home_resp.status_code == 200:
            logger.info(f"   ✅ Cookies set: {len(session.cookies)}")
        else:
            logger.warning(f"   ⚠️ Cookie warmup status: {home_resp.status_code}")
    except Exception as e:
        logger.warning(f"   ⚠️ Cookie warmup error: {e}")

    urls = [
        f"https://old.reddit.com/r/{subreddit}/",
        f"https://old.reddit.com/r/{subreddit}/hot/",
        f"https://www.reddit.com/r/{subreddit}/",
        f"https://www.reddit.com/r/{subreddit}/hot/",
    ]

    for url_index, url in enumerate(urls, 1):
        try:
            logger.info(f"🌐 Attempt {url_index}/{len(urls)}: {url}")
            response = session.get(url, timeout=30, allow_redirects=True)
            logger.info(f"   Status: {response.status_code}")

            if response.status_code == 403:
                logger.warning("   ❌ 403 Forbidden – trying next URL…")
                continue

            response.raise_for_status()
            logger.info(f"   📄 HTML received: {len(response.text)} bytes")
            posts = parse_reddit_html(response.text)

            if not posts:
                logger.warning("   ⚠️ No posts extracted from HTML")
                continue

            posts = posts[:limit]
            logger.info(f"✅ {len(posts)} posts extracted from r/{subreddit}!")
            for i, post in enumerate(posts[:3]):
                logger.info(f"   Post {i+1}: {post['title'][:50]}… (ID: {post['id']})")
            return posts

        except requests.exceptions.HTTPError as e:
            logger.warning(f"❌ HTTP {e.response.status_code} on attempt {url_index}")
            continue
        except Exception as e:
            logger.warning(f"❌ Failure on attempt {url_index}: {e}")
            continue

    logger.error("❌ All attempts failed for Reddit HTML scraping")
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def extractContent():
    """
    Fetch a single new post for the bot to tweet.

    LOGIC:
    1. If any proxy is ONLINE → always fetch a fresh batch (FIFO rotation).
    2. If all proxies are OFFLINE → consume existing saved batches.
    3. Returns a list with exactly 1 post, or an empty list.
    """
    initialize_queue_db()

    stats = get_queue_stats()
    logger.info("=" * 60)
    logger.info("📊 Queue Status:")
    logger.info(f"   Stored batches : {stats['batches_count']}/{MAX_JSON_BATCHES}")
    logger.info(f"   Available posts: {stats['available_posts']}")
    logger.info(f"   Total posted   : {stats['posted_total']}")
    if stats['batches']:
        for batch in stats['batches']:
            logger.info(
                f"   • Batch #{batch['batch_id']}: r/{batch['subreddit']} "
                f"– {batch['remaining']}/{batch['total']} remaining"
            )
    logger.info("=" * 60)

    proxy_available = check_proxy_available()

    if proxy_available:
        logger.info("🟢 ONLINE MODE: fetching fresh batch via proxy (HTML scraping)…")
        subreddit = choice(['Overwatch', 'Overwatch_Memes'])
        logger.info(f"🎲 Selected subreddit: r/{subreddit}")
        posts = fetch_posts_from_reddit_html(subreddit, limit=50)
        if posts:
            batch_id = add_json_batch(posts, subreddit)
            logger.info(f"💾 Batch #{batch_id} saved with {len(posts)} posts")
            stats = get_queue_stats()
            logger.info(
                f"📊 Queue updated: {stats['batches_count']} batch(es), "
                f"{stats['available_posts']} posts available"
            )
        else:
            logger.warning("⚠️ Failed to fetch new posts – using saved batches as fallback")
    else:
        logger.info("🔴 OFFLINE MODE: all proxies unavailable, consuming saved batches")

    logger.info("🔍 Looking for next unseen post…")
    batch_id, post = get_next_unposted_post()

    if post:
        logger.info(f"✨ Post found in batch #{batch_id}")
        logger.info(f"📤 Title: {post['title'][:70]}…")
        return [post]
    else:
        logger.error("❌ EMPTY QUEUE! No new posts available.")
        if proxy_available:
            logger.error("   Unexpected – posts were fetched but none are new.")
        else:
            logger.error("   Waiting for a proxy to come back online.")
        return []


def debug_data(posts):
    if posts:
        for post in posts:
            print("\n🔹 Selected post:")
            print(f"  ID    : {post['id']}")
            print(f"  Title : {post['title'][:70]}…")
            print(f"  URL   : {post['url']}")
            if post['s_img']:
                print("  Image : Yes (single)")
            if post['m_img']:
                print(f"  Gallery: {len(post['m_img'])} images")
            if post['video']:
                print("  Video : Yes")
        print()
    else:
        print("\n❌ No posts available.\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print("=" * 60)
    print("Reddit HTML Scraping System (multi-proxy)")
    print(f"Max batches: {MAX_JSON_BATCHES}")
    print("=" * 60)
    posts = extractContent()
    debug_data(posts)