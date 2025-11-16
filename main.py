import tweepy, logging, requests, os, time, subprocess, sys, json
from contextlib import closing
from logging.handlers import RotatingFileHandler

from oauth import *
from reddit import extractContent
from database import get_db_connection

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
    with closing(get_db_connection(DB_PATH)) as conn, conn:
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
    with closing(get_db_connection(DB_PATH)) as conn:
        cur = conn.execute("SELECT 1 FROM seen_posts WHERE post_id = ?", (post_id,))
        return cur.fetchone() is not None

def mark_post_as_seen(post_id: str) -> None:
    with closing(get_db_connection(DB_PATH)) as conn, conn:
        conn.execute("INSERT OR IGNORE INTO seen_posts(post_id) VALUES(?)", (post_id,))
        conn.execute("DELETE FROM pending_posts WHERE post_id = ?", (post_id,))

def remove_pending_post(post_id: str) -> None:
    with closing(get_db_connection(DB_PATH)) as conn, conn:
        conn.execute("DELETE FROM pending_posts WHERE post_id = ?", (post_id,))
        logger.info("Removed pending post %s from DB", post_id)

def save_pending_post(post_id: str, content: str, img_paths: list[str], video_path: str) -> None:
    img_paths_json = json.dumps(img_paths if img_paths else [])
    with closing(get_db_connection(DB_PATH)) as conn, conn:
        # ensure there is at most one pending row
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
    with closing(get_db_connection(DB_PATH)) as conn:
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
    try:
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        logger.info("Downloaded %s -> %s", url, filename)
        return filename
    except Exception as exc:
        logger.error("Download failed for %s: %s", url, exc)
        return None


def combine_video_audio(video_path: str, audio_path: str, output_path: str) -> str | None:
    cmd = [
        "ffmpeg",
        "-y",  # overwrite
        "-i",
        video_path,
        "-i",
        audio_path,
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-strict",
        "experimental",
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
            logging.warning(f"Could not read rate‑limit for {endpoint}")
            return

        limit = resource_block[full_ep]
        if limit["remaining"] <= 10:
            sleep_time = limit["reset"] - time.time()
            if sleep_time > 0:
                logging.info(f"Approaching rate limit for {endpoint}. "
                             f"Sleeping {sleep_time:.0f}s.")
                time.sleep(sleep_time)
    except tweepy.TweepyException as e:
        logging.error(f"Failed to check rate limits: {e}")

# ---------- Twitter logic ----------


def _is_unrecoverable_tweepy_error(exc: Exception) -> bool:
    """Return True if the error indicates the post should be discarded instead of retried."""
    msg = str(exc).lower()
    # invalid media ids (common)
    if "your media ids are invalid" in msg or "media ids are invalid" in msg or "invalid media id" in msg:
        return True
    # media too large / duration too long
    if "duration" in msg and ("too" in msg or "exceed" in msg or "long" in msg):
        return True
    if "video duration" in msg or "video is too long" in msg or "exceeds the maximum" in msg:
        return True
    # other 4xx unrecoverable errors
    if "400" in msg or "403" in msg and "forbidden" in msg:
        # be conservative; only some 4xx are unrecoverable — we already matched typical messages above
        return False
    return False


def post_to_twitter(text: str, img_paths: list[str], video_path: str, post_id: str | None = None) -> tuple[bool, bool]:
    """Attempt to post to Twitter.
    Returns (success, fatal) where fatal=True means "don't retry / delete pending".
    """
    media_ids: list[int] = []

    try:
        # video handling: support CMAF (muxed) or older DASH (separate audio)
        if video_path:
            video_url = video_path
            needs_merge = "DASH_" in video_url and "DASH_AUDIO_128.mp4" not in video_url
            # try to discover audio URL for old format
            if needs_merge:
                base_url = "/".join(video_url.split("/")[:-1])
                audio_url = f"{base_url}/DASH_AUDIO_128.mp4"
                v_file = download_media(video_url, "temp_video.mp4")
                a_file = download_media(audio_url, "temp_audio.mp4")
                if v_file and a_file:
                    combo_file = combine_video_audio(v_file, a_file, "temp_combined.mp4")
                    if combo_file:
                        check_rate_limits(api, "/media/upload")
                        media = api.media_upload(combo_file, media_category="tweet_video", chunked=True)
                        media_ids.append(media.media_id)
                # cleanup
                for f in (v_file, a_file, "temp_combined.mp4"):
                    try:
                        if f and os.path.exists(f):
                            os.remove(f)
                    except FileNotFoundError:
                        pass
            else:
                # CMAF or packaged media (likely already muxed)
                v_file = download_media(video_url, "temp_video.mp4")
                if v_file:
                    try:
                        check_rate_limits(api, "/media/upload")
                        media = api.media_upload(v_file, media_category="tweet_video", chunked=True)
                        media_ids.append(media.media_id)
                    finally:
                        try:
                            if v_file and os.path.exists(v_file):
                                os.remove(v_file)
                        except FileNotFoundError:
                            pass

        # images
        elif img_paths:
            for idx, url in enumerate(img_paths[:4]):
                local = download_media(url, f"temp_image_{idx}.jpg")
                if local:
                    check_rate_limits(api, "/media/upload")
                    media = api.media_upload(local)
                    media_ids.append(media.media_id)
                    os.remove(local)

        # tweet
        if text or media_ids:
            resp = client.create_tweet(
                text=text,
                media_ids=media_ids if media_ids else None,
                user_auth=True,
            )
            logger.info("Tweet posted: %s", resp.data["id"])
            return True, False
        else:
            logger.error("Nothing to tweet: no text/media")
            return False, False
    except tweepy.TweepyException as exc:
        logger.error("Tweepy error: %s", exc)
        fatal = _is_unrecoverable_tweepy_error(exc)
        if fatal and post_id:
            # drop the pending post so it won't block future runs
            remove_pending_post(post_id)
            logger.info("Dropped pending post %s due to unrecoverable error: %s", post_id, exc)
            return False, True
        return False, False
    except Exception as exc:
        logger.error("Unexpected error in post_to_twitter: %s", exc)
        return False, False

# ---------- Orchestration ----------

def process_posts() -> None:
    pending = get_pending_posts()
    # ---------- Try pendings ----------
    if pending:
        p = pending[0]
        success, fatal = post_to_twitter(p["content"], p["img_paths"], p["video_path"], post_id=p["post_id"])
        if success:
            mark_post_as_seen(p["post_id"])
            return
        else:
            if fatal:
                logger.info("Pending post %s removed (fatal)", p["post_id"])
                return
            logger.info("Retry failed for %s", p["post_id"])
            return
        
    # ---------- Look for a new post ----------
    for post in extractContent():
        if is_post_seen(post["id"]):
            continue

        # Build images/video
        img_paths: list[str] = []
        if post.get("s_img"):
            img_paths.append(post["s_img"])
        elif post.get("m_img"):
            img_paths.extend(post["m_img"])

        video_path = post.get("video", "")

        # Build text
        post_content = (post.get("title", "") + "\n" + post.get("content", "")).strip()
        if isinstance(post_content, bytes):
            post_content = post_content.decode('utf-8', errors='replace')
        post_url = post.get("url", "")
        if post_content and post_url:
            limit = 277 - len(post_url)
            content = f"{post_content[:limit]}...\n{post_url}" if len(post_content) > limit else f"{post_content}\n{post_url}"
        else:
            content = (post_content or post_url)[:280]

        content = content.encode('utf-8', errors='replace').decode('utf-8') if content else ""
        success, fatal = post_to_twitter(content, img_paths, video_path, post_id=post["id"])
        if success:
            mark_post_as_seen(post["id"])
            return
        else:
            if fatal:
                logger.info("New post %s not saved because error is fatal", post["id"])
                return
            save_pending_post(post["id"], content, img_paths, video_path)
            logger.info("Saved %s for retry", post["id"])
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
        logger.error("Main loop error: %s", repr(exc))
        logger.error("Full traceback:", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
