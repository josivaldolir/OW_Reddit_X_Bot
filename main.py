import tweepy, logging, requests, os, time, subprocess, sys, json, re
from contextlib import closing
from logging.handlers import RotatingFileHandler

from oauth import *
from reddit import extractContent
from database import get_db_connection
from proxy_manager import get_available_proxy, get_requests_proxies, is_any_proxy_available

def download_media_no_proxy(url: str, filename: str) -> str | None:
    """
    Download de imagens SEM proxy (conexão direta).
    Usado para imagens do Reddit que são públicas.
    """
    try:
        if url.startswith('//'):
            url = 'https:' + url

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        resp = requests.get(url, stream=True, timeout=30, headers=headers)
        resp.raise_for_status()

        with open(filename, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)

        logger.info(f"✅ Downloaded (no proxy): {filename} ({len(resp.content)} bytes)")
        return filename
    except Exception as exc:
        logger.error(f"❌ Download failed for {url}: {exc}")
        return None


def check_proxy_available() -> bool:
    """
    Returns True if at least one configured proxy is reachable.
    Delegates to proxy_manager so all proxy logic stays centralised.
    """
    return is_any_proxy_available()


# <-- yt-dlp -->
try:
    import yt_dlp
except Exception as e:
    yt_dlp = None
    logging.getLogger(__name__).warning("yt_dlp não disponível: %s", e)

# ---------- logging ----------
stream_handler = logging.StreamHandler(sys.stdout)

log_handler = RotatingFileHandler(
    "twitter_bot.log", maxBytes=1_000_000, backupCount=5, encoding="utf-8"
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[log_handler, stream_handler],
)
logger = logging.getLogger(__name__)

# ---------- Tweepy ----------
client = tweepy.Client(
    bearer_token=bearer_token,
    consumer_key=api_key,
    consumer_secret=api_secret,
    access_token=access_token,
    access_token_secret=access_token_secret,
)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# ---------- DB helpers ----------

DB_PATH = "seen_posts.db"


def initialize_db() -> None:
    with closing(get_db_connection()) as conn, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_posts (
                post_id     TEXT PRIMARY KEY,
                content     TEXT,
                img_paths   TEXT,
                video_path  TEXT,
                attempts    INTEGER DEFAULT 0,
                last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS seen_posts (post_id TEXT PRIMARY KEY);"
        )


def is_post_seen(post_id: str) -> bool:
    with closing(get_db_connection()) as conn:
        cur = conn.execute("SELECT 1 FROM seen_posts WHERE post_id = ?", (post_id,))
        return cur.fetchone() is not None


def mark_post_as_seen(post_id: str) -> None:
    with closing(get_db_connection()) as conn, conn:
        conn.execute("INSERT OR IGNORE INTO seen_posts(post_id) VALUES(?)", (post_id,))
        conn.execute("DELETE FROM pending_posts WHERE post_id = ?", (post_id,))


def remove_pending_post(post_id: str) -> None:
    with closing(get_db_connection()) as conn, conn:
        conn.execute("DELETE FROM pending_posts WHERE post_id = ?", (post_id,))
        logger.info("Removed pending post %s from DB", post_id)


def save_pending_post(post_id: str, content: str, img_paths: list[str], video_path: str) -> None:
    img_paths_json = json.dumps(img_paths if img_paths else [])
    with closing(get_db_connection()) as conn, conn:
        conn.execute("DELETE FROM pending_posts;")
        conn.execute(
            """
            INSERT INTO pending_posts (post_id, content, img_paths, video_path, attempts, last_attempt)
            VALUES (?, ?, ?, ?, 0, CURRENT_TIMESTAMP)
            """,
            (post_id, content, img_paths_json, video_path),
        )


def _parse_img_paths(img_paths_json: str) -> list[str]:
    try:
        return json.loads(img_paths_json) if img_paths_json else []
    except Exception:
        return []


def get_pending_posts() -> list[dict]:
    with closing(get_db_connection()) as conn:
        cur = conn.execute(
            """
            SELECT post_id, content, img_paths, video_path
            FROM pending_posts
            WHERE attempts < 3
            ORDER BY last_attempt ASC;
            """
        )
        return [
            {
                "post_id": row[0],
                "content": row[1],
                "img_paths": _parse_img_paths(row[2]),
                "video_path": row[3],
            }
            for row in cur.fetchall()
        ]


# ---------- utils ----------

def download_media(url: str, filename: str) -> str | None:
    """Direct (no-proxy) download for public Reddit images."""
    try:
        if url.startswith('//'):
            url = 'https:' + url

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        if any(ext in url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
            logger.info(f"📥 Downloading image (direct): {url}")

        resp = requests.get(url, stream=True, timeout=30, headers=headers)
        resp.raise_for_status()

        total_bytes = 0
        with open(filename, "wb") as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)

        logger.info(f"✅ Downloaded: {filename} ({total_bytes} bytes)")
        return filename

    except requests.exceptions.HTTPError as exc:
        logger.error(f"❌ HTTP error downloading {url}: {exc.response.status_code}")
        return None
    except requests.exceptions.Timeout:
        logger.error(f"❌ Timeout downloading {url}")
        return None
    except Exception as exc:
        logger.error(f"❌ Download failed for {url}: {exc}")
        return None


def combine_video_audio(video_path: str, audio_path: str, output_path: str) -> str | None:
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-i", audio_path,
        "-c:v", "copy",
        "-c:a", "aac",
        "-strict", "experimental",
        output_path,
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("Combined video/audio -> %s", output_path)
        return output_path
    except subprocess.CalledProcessError as exc:
        logger.error("ffmpeg error: %s", exc.stderr.decode(errors="ignore")[:300])
        return None


def check_rate_limits(api, endpoint):
    try:
        rate_limit_status = api.rate_limit_status()
        resource, ep = endpoint.lstrip("/").split("/", 1)
        full_ep = f"/{resource}/{ep}"
        resource_block = rate_limit_status["resources"].get(resource)
        if not resource_block or full_ep not in resource_block:
            logging.warning(f"Could not read rate-limit for {endpoint}")
            return

        limit = resource_block[full_ep]
        if limit["remaining"] <= 10:
            sleep_time = limit["reset"] - time.time()
            if sleep_time > 0:
                logging.info(
                    f"Approaching rate limit for {endpoint}. Sleeping {sleep_time:.0f}s."
                )
                time.sleep(sleep_time)
    except tweepy.TweepyException as e:
        logging.error(f"Failed to check rate limits: {e}")


def check_audio_stream(video_path: str) -> bool:
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_type",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return "audio" in result.stdout.lower()
    except Exception as e:
        logger.warning(f"Não foi possível verificar áudio com ffprobe: {e}")
        return False


# ---------- yt-dlp ----------

def download_reddit_video_ytdlp_auth(
    url: str, output_filename: str = "temp_video.mp4"
) -> tuple[str | None, int | None, str | None]:
    """
    Download a Reddit video using yt-dlp.

    Proxy selection: tries each configured proxy in order via proxy_manager.
    Falls back to a direct connection if none are available.

    Returns (filename_or_none, duration_seconds_or_none, error_message_or_none)
    """
    if yt_dlp is None:
        msg = "yt_dlp not installed"
        logger.error(msg)
        return None, None, msg

    try:
        logger.info(f"Using yt-dlp for: {url}")

        # --- Proxy selection ---
        active_proxy = get_available_proxy()
        proxy_config = {}
        if active_proxy:
            proxy_config["proxy"] = active_proxy["url"]
            logger.info(f"Using {active_proxy['label']} in yt-dlp")
        else:
            logger.warning("⚠️  All proxies offline – yt-dlp will use direct connection")

        ydl_opts = {
            "outtmpl": output_filename,
            "format": "bv*+ba/b",
            "merge_output_format": "mp4",
            "postprocessors": [{
                "key": "FFmpegVideoConvertor",
                "preferedformat": "mp4",
            }],
            "postprocessor_args": ["-c:v", "copy", "-c:a", "aac", "-b:a", "128k"],
            "quiet": False,
            "no_warnings": False,
            "verbose": True,
            "prefer_ffmpeg": True,
            "http_headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                )
            },
            "nocheckcertificate": True,
            **proxy_config,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info("Extracting video info…")
            info = ydl.extract_info(url, download=False)
            duration = info.get("duration")

            if "formats" in info:
                logger.info(f"Available formats: {len(info['formats'])}")
                for fmt in info["formats"][:5]:
                    has_video = fmt.get('vcodec', 'none') != 'none'
                    has_audio = fmt.get('acodec', 'none') != 'none'
                    logger.info(
                        f"  - {fmt.get('format_id')}: "
                        f"video={has_video} audio={has_audio} ext={fmt.get('ext')}"
                    )

            if duration and duration > 140:
                logger.info(f"Video too long: {duration}s > 140s")
                return None, duration, "too_long"

            logger.info("Downloading video with audio…")
            ydl.download([url])

            if os.path.exists(output_filename):
                file_size = os.path.getsize(output_filename)
                logger.info(f"✓ Download complete: {output_filename} ({file_size} bytes)")

                has_audio = check_audio_stream(output_filename)
                if not has_audio:
                    logger.warning("⚠️ No audio stream detected – attempting manual merge")
                    return try_manual_audio_merge(url, output_filename)
                else:
                    logger.info("✓ Audio confirmed!")

                return output_filename, duration, None
            else:
                logger.error("File not created after download")
                return None, duration, "download_failed_no_file"

    except Exception as exc:
        logger.error(f"yt-dlp error: {exc}", exc_info=True)
        return try_manual_audio_merge(url, output_filename)


def try_manual_audio_merge(
    post_url: str, video_file: str
) -> tuple[str | None, int | None, str | None]:
    """
    Fallback: fetch video/audio URLs from the Reddit JSON API and merge with ffmpeg.

    Uses proxy_manager for the JSON fetch so it benefits from automatic proxy fallback.
    """
    try:
        logger.info("Attempting manual audio merge…")

        match = re.search(r'/comments/([a-z0-9]+)/', post_url)
        if not match:
            logger.error("Invalid post URL")
            return None, None, "invalid_post_url"

        post_id = match.group(1)

        # --- Proxy selection ---
        active_proxy = get_available_proxy()
        proxies = get_requests_proxies(active_proxy)   # None = direct
        verify_ssl = True

        if active_proxy:
            logger.info(f"Using {active_proxy['label']} for Reddit JSON fetch")
            verify_ssl = False  # Disable SSL verification when routing through proxy
        else:
            logger.warning("All proxies offline – fetching Reddit JSON directly")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        # Fetch post JSON
        try:
            json_url = f"https://www.reddit.com/comments/{post_id}.json"
            logger.info(f"Fetching post JSON: {json_url}")
            response = requests.get(
                json_url,
                headers=headers,
                proxies=proxies,
                verify=verify_ssl,
                timeout=15,
            )
            response.raise_for_status()
            json_data = response.json()
            post_data = json_data[0]['data']['children'][0]['data']

        except Exception as e:
            logger.error(f"Error fetching post JSON: {e}")
            error_str = str(e).lower()
            if "402" in error_str or "bad_endpoint" in error_str or "residential failed" in error_str:
                logger.error("Proxy does not support this URL – marking as fatal")
                return None, None, "proxy_endpoint_not_supported_fatal"
            return None, None, "json_fetch_failed"

        if 'media' not in post_data or 'reddit_video' not in post_data.get('media', {}):
            logger.error("Post has no video metadata")
            return None, None, "no_video_metadata"

        fallback_url = post_data['media']['reddit_video'].get('fallback_url', '')
        if not fallback_url:
            logger.error("No fallback_url found")
            return None, None, "no_fallback_url"

        logger.info(f"Fallback URL: {fallback_url}")

        # Download video if not already on disk
        if not os.path.exists(video_file):
            logger.info("Downloading video from fallback_url…")
            resp = requests.get(
                fallback_url,
                timeout=60,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            )
            resp.raise_for_status()
            with open(video_file, 'wb') as f:
                f.write(resp.content)
            logger.info(f"Video downloaded: {len(resp.content)} bytes")

        # Try audio URLs (CMAF first, then legacy DASH)
        base_url = fallback_url.rsplit('/', 1)[0]
        audio_urls = [
            f"{base_url}/CMAF_AUDIO_128.mp4",
            f"{base_url}/CMAF_AUDIO_64.mp4",
            f"{base_url}/DASH_AUDIO_128.mp4",
            f"{base_url}/DASH_AUDIO_64.mp4",
            f"{base_url}/DASH_audio.mp4",
            f"{base_url}/audio.mp4",
        ]

        audio_file = None
        for audio_url in audio_urls:
            try:
                logger.info(f"Trying audio: {audio_url}")
                resp = requests.get(
                    audio_url,
                    timeout=30,
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
                )
                if resp.status_code == 200 and len(resp.content) > 1000:
                    audio_file = "temp_audio.mp4"
                    with open(audio_file, 'wb') as f:
                        f.write(resp.content)
                    logger.info(f"✓ Audio downloaded: {len(resp.content)} bytes")
                    break
            except Exception as e:
                logger.debug(f"Failed {audio_url}: {e}")
                continue

        if not audio_file:
            logger.error("Could not find audio stream at any URL")
            try:
                if os.path.exists(video_file):
                    os.remove(video_file)
            except Exception as e:
                logger.warning(f"Cleanup failed: {e}")
            return None, None, "audio_not_found_fatal"

        # Merge
        output_file = "temp_video_merged.mp4"
        result = combine_video_audio(video_file, audio_file, output_file)

        try:
            os.remove(audio_file)
        except Exception:
            pass

        if result and os.path.exists(output_file):
            try:
                os.remove(video_file)
            except Exception:
                pass
            os.rename(output_file, video_file)
            logger.info("✓ Manual audio merge successful!")
            return video_file, None, None
        else:
            logger.error("Merge failed")
            return None, None, "merge_failed"

    except Exception as exc:
        logger.error(f"Error in manual merge: {exc}", exc_info=True)
        return None, None, str(exc)


# ---------- Twitter logic ----------

def _is_unrecoverable_tweepy_error(exc: tweepy.TweepyException) -> bool:
    resp = getattr(exc, "response", None)
    if resp is not None:
        code = getattr(resp, "status_code", None)
        if code in (400, 403):
            try:
                data = resp.json()
                msg = str(data)
            except Exception:
                msg = resp.text or ""
            msg_lower = msg.lower()
            fatal_markers = [
                "not allowed to post a video longer",
                "your media ids are invalid",
                "media id is invalid",
                "unsupported",
                "file type not supported",
                "duration",
                "too long",
                "invalid media",
                "video too long",
                "403 forbidden",
            ]
            if any(m in msg_lower for m in fatal_markers):
                return True

    msg = str(exc).lower()
    fatal_markers = [
        "not allowed to post a video longer",
        "your media ids are invalid",
        "media id is invalid",
        "unsupported",
        "file type not supported",
        "duration",
        "too long",
        "invalid media",
        "video too long",
        "403 forbidden",
    ]
    return any(m in msg for m in fatal_markers)


def post_to_twitter(
    text: str, img_paths: list[str], video_path: str, post_id: str | None = None
) -> tuple[bool, bool]:
    """
    Attempt to post to Twitter.
    Returns (success, fatal) where fatal=True means "don't retry / delete pending".
    """
    media_ids: list[int] = []

    try:
        # VIDEO
        if video_path:
            proxy_online = check_proxy_available()

            if not proxy_online:
                logger.warning("⚠️  All proxies OFFLINE – cannot download video")
                logger.info(f"⏭️  Skipping video post {post_id} – moving to next")
                if post_id:
                    mark_post_as_seen(post_id)
                return False, True

            logger.info("✅ Proxy online – downloading video…")
            out_file = "temp_video.mp4"
            filename, duration, err = download_reddit_video_ytdlp_auth(video_path, out_file)

            if err == "too_long":
                logger.info(f"Video too long (>140s). Fatal for post_id={post_id}")
                if post_id:
                    remove_pending_post(post_id)
                return False, True

            if filename is None:
                logger.error(f"Download failed for {video_path}: {err}")
                fatal_errors = [
                    "copyright", "404", "forbidden", "not permitted", "unavailable",
                    "audio_not_found_fatal", "no_video_metadata", "invalid_post_url",
                    "proxy_endpoint_not_supported_fatal", "bad_endpoint",
                ]
                if err and any(k in err.lower() for k in fatal_errors):
                    if post_id:
                        remove_pending_post(post_id)
                    return False, True
                return False, False

            try:
                check_rate_limits(api, "/media/upload")
                logger.info(f"Uploading video to Twitter: {filename}")
                media = api.media_upload(filename, media_category="tweet_video", chunked=True)
                media_ids.append(media.media_id)
                logger.info(f"✓ Video uploaded! Media ID: {media.media_id}")
            except Exception as exc:
                logger.error(f"Error uploading video {filename}: {exc}", exc_info=True)
                if post_id and _is_unrecoverable_tweepy_error(exc):
                    remove_pending_post(post_id)
                    return False, True
                return False, False
            finally:
                try:
                    if filename and os.path.exists(filename):
                        os.remove(filename)
                except Exception as e:
                    logger.warning(f"Failed to cleanup {filename}: {e}")

        # IMAGES (direct connection – Reddit images are public)
        elif img_paths:
            logger.info(f"📸 Downloading {len(img_paths)} image(s) (direct connection)…")
            downloaded_count = 0
            for idx, url in enumerate(img_paths[:4]):
                if not url or not url.strip():
                    logger.warning(f"⚠️ Image {idx+1}: empty URL, skipping")
                    continue
                if url.startswith('//'):
                    url = 'https:' + url
                if not any(domain in url for domain in ['i.redd.it', 'preview.redd.it']):
                    logger.warning(f"⚠️ Image {idx+1}: invalid URL ({url[:50]}…), skipping")
                    continue
                logger.info(f"📥 Downloading image {idx+1}/{len(img_paths[:4])}: {url[:80]}…")
                local = download_media(url, f"temp_image_{idx}.jpg")
                if local:
                    try:
                        check_rate_limits(api, "/media/upload")
                        media = api.media_upload(local)
                        media_ids.append(media.media_id)
                        downloaded_count += 1
                        logger.info(f"✅ Image {idx+1} uploaded (Media ID: {media.media_id})")
                    except Exception as exc:
                        logger.error(f"❌ Failed to upload image {idx+1}: {exc}")
                    finally:
                        try:
                            os.remove(local)
                        except Exception:
                            pass
                else:
                    logger.warning(f"⚠️ Failed to download image {idx+1}")

            if downloaded_count == 0:
                logger.error("❌ No images downloaded successfully")
            else:
                logger.info(f"✅ Processed {downloaded_count}/{len(img_paths[:4])} image(s)")

        # TWEET
        if text or media_ids:
            resp = client.create_tweet(
                text=text,
                media_ids=media_ids if media_ids else None,
                user_auth=True,
            )
            logger.info(f"✓ Tweet posted: {resp.data['id']}")
            return True, False

        logger.error("Nothing to tweet: no text or media")
        return False, False

    except tweepy.TweepyException as exc:
        logger.error(f"Tweepy error: {exc}", exc_info=True)
        fatal = _is_unrecoverable_tweepy_error(exc)
        if fatal and post_id:
            remove_pending_post(post_id)
            return False, True
        return False, False

    except Exception as exc:
        logger.error(f"Unexpected error in post_to_twitter: {exc}", exc_info=True)
        return False, False


# ---------- Orchestration ----------

def process_posts() -> None:
    pending = get_pending_posts()

    if pending:
        p = pending[0]
        success, fatal = post_to_twitter(
            p["content"], p["img_paths"], p["video_path"], post_id=p["post_id"]
        )
        if success:
            mark_post_as_seen(p["post_id"])
            return
        if fatal:
            logger.info(f"Pending post {p['post_id']} removed (fatal)")
            return
        logger.info(f"Retry failed for {p['post_id']} (non-fatal)")
        return

    try:
        posts = extractContent()
    except Exception as exc:
        logger.error(f"Error fetching Reddit posts: {exc}")
        logger.warning("Skipping this run due to Reddit API error")
        return

    for post in posts:
        if is_post_seen(post["id"]):
            continue

        img_paths: list[str] = []
        if post.get("m_img"):
            img_paths = post["m_img"][:4]
            logger.info(f"📸 Post has gallery with {len(img_paths)} image(s)")
        elif post.get("s_img"):
            img_paths = [post["s_img"]]
            logger.info("📸 Post has 1 single image")

        img_paths = [url for url in img_paths if url and url.strip()]

        if img_paths:
            logger.info("📋 Image URLs to download:")
            for i, url in enumerate(img_paths, 1):
                logger.info(f"   {i}. {url[:80]}…")

        video_path = post.get("video", "")
        post_content = (post.get("title", "") + "\n" + post.get("content", "")).strip()

        if isinstance(post_content, bytes):
            post_content = post_content.decode("utf-8", errors="replace")

        post_url = post.get("url", "")

        if post_content and post_url:
            limit = 277 - len(post_url)
            content = (
                f"{post_content[:limit]}...\n{post_url}"
                if len(post_content) > limit
                else f"{post_content}\n{post_url}"
            )
        else:
            content = (post_content or post_url)[:280]

        content = (
            content.encode("utf-8", errors="replace").decode("utf-8") if content else ""
        )

        success, fatal = post_to_twitter(
            content, img_paths, video_path, post_id=post["id"]
        )

        if success:
            mark_post_as_seen(post["id"])
            return

        if fatal:
            logger.info(f"New post {post['id']} permanently ignored (fatal error)")
            mark_post_as_seen(post["id"])
            return

        save_pending_post(post["id"], content, img_paths, video_path)
        logger.info(f"Saved {post['id']} for retry")
        return


# ---------- main ----------

def main():
    try:
        initialize_db()
        process_posts()
        logger.info("Done.")
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as exc:
        logger.error(f"Main loop error: {repr(exc)}")
        logger.error("Full traceback:", exc_info=True)
        logger.warning("Bot will retry on next scheduled run")


if __name__ == "__main__":
    main()