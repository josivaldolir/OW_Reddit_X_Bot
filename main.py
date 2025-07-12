import tweepy, logging, requests, os, time, subprocess, sys
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
    with closing(get_db_connection()) as conn, conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_posts (
                post_id     TEXT PRIMARY KEY,
                content     TEXT,
                img_paths   TEXT,           -- JSON‑like string ["url1","url2"]
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
        conn.execute("DELETE FROM pending_posts WHERE post_id=?", (post_id,))


def save_pending_post(post_id: str, content: str, img_paths: list[str], video_path: str) -> None:
    img_paths_json = (
        "[]" if not img_paths else "[" + ",".join(f'"{img}"' for img in img_paths) + "]"
    )
    with closing(get_db_connection()) as conn, conn:
        conn.execute(
            """
            INSERT INTO pending_posts (post_id, content, img_paths, video_path, attempts, last_attempt)
            VALUES (
                :pid,
                :content,
                :imgs,
                :video,
                COALESCE((SELECT attempts + 1 FROM pending_posts WHERE post_id = :pid), 0),
                CURRENT_TIMESTAMP
            )
            ON CONFLICT(post_id) DO UPDATE SET
                content=excluded.content,
                img_paths=excluded.img_paths,
                video_path=excluded.video_path,
                attempts=pending_posts.attempts+1,
                last_attempt=CURRENT_TIMESTAMP;
            """,
            {
                "pid": post_id,
                "content": content,
                "imgs": img_paths_json,
                "video": video_path,
            },
        )


def _parse_img_paths(img_paths_json: str) -> list[str]:
    if img_paths_json == "[]":
        return []
    return [p.strip("\"") for p in img_paths_json[1:-1].split(",")]


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
        resource, ep = endpoint.lstrip('/').split('/', 1)
        full_ep = f'/{ep}'
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

def post_to_twitter(text: str, img_paths: list[str], video_path: str) -> bool:
    media_ids: list[int] = []

    try:
        # vídeo
        if video_path:
            video_url = video_path
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
            for f in (v_file, a_file, "temp_combined.mp4"):
                try:
                    if f and os.path.exists(f):
                        os.remove(f)
                except FileNotFoundError:
                    pass

        # imagens
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
            check_rate_limits(api, "/statuses/update")
            resp = client.create_tweet(
                text=text,
                media_ids=media_ids if media_ids else None,
                user_auth=True
            )
            logger.info("Tweet posted: %s", resp.data["id"])
            return True
        else:
            logger.error("Nothing to tweet: no text/media")
            return False
    except tweepy.TweepyException as exc:
        logger.error("Tweepy error: %s", exc)
        return False
    except Exception as exc:
        logger.error("Unexpected error in post_to_twitter: %s", exc)
        return False

# ---------- Orchestration ----------

def process_posts() -> None:
    # 1. try pending posts
    for p in get_pending_posts():
        ok = post_to_twitter(p["content"], p["img_paths"], p["video_path"])
        if ok:
            mark_post_as_seen(p["post_id"])
        else:
            logger.info("Retry failed for %s", p["post_id"])

    # 2. new posts
    for post in extractContent():
        if is_post_seen(post["id"]):
            continue

        img_paths: list[str] = []
        if post.get("s_img"):
            img_paths.append(post["s_img"])
        elif post.get("m_img"):
            img_paths.extend(post["m_img"])

        video_path = post.get("video", "")
        post_content = post.get("title", "") + "\n" + post.get("content", "")
        post_url = post.get("url", "")

        # mounts the tweet
        if post_content and post_url:
            limit = 277 - len(post_url)
            content = f"{post_content[:limit]}...\n{post_url}" if len(post_content) > limit else f"{post_content}\n{post_url}"
        else:
            content = (post_content or post_url)[:280]

        ok = post_to_twitter(content, img_paths, video_path)
        if ok:
            mark_post_as_seen(post["id"])
        else:
            save_pending_post(post["id"], content, img_paths, video_path)
            logger.info("Saved %s for retry", post["id"])

# ---------- main ----------

def main():
    try:
        initialize_db()
        process_posts()
        logger.info("Done.")
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as exc:
        logger.error("Main loop error: %s", exc)
        sys.exit(1)

if __name__ == "__main__":
    main()
