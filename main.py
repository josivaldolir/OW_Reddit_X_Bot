import tweepy, logging, requests, os, time, subprocess, sys, json
from contextlib import closing
from logging.handlers import RotatingFileHandler

from oauth import *
from reddit import extractContent
from database import get_db_connection
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin, parse_qs

# <-- NEW: yt-dlp -->
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
        "-y",
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
            logging.warning(f"Could not read rate-limit for {endpoint}")
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

# ---------- yt-dlp integration (CORRIGIDO) ----------

def download_reddit_video_ytdlp(url: str, output_filename: str = "temp_video.mp4") -> tuple[str | None, int | None, str | None]:
    """
    Uses yt-dlp to download reddit video WITH AUDIO properly merged.
    Returns (filename_or_none, duration_seconds_or_none, error_message_or_none)
    
    CORREÇÃO: Agora força o download de melhor vídeo + melhor áudio e faz merge via ffmpeg
    """
    if yt_dlp is None:
        msg = "yt_dlp not installed"
        logger.error(msg)
        return None, None, msg

    # OPÇÕES CORRIGIDAS para garantir áudio
    ydl_opts = {
        "outtmpl": output_filename,
        "format": "bestvideo+bestaudio/best",  # Força download de vídeo E áudio separados
        "merge_output_format": "mp4",  # Merge para MP4
        "postprocessors": [{
            "key": "FFmpegVideoConvertor",
            "preferedformat": "mp4",
        }],
        "quiet": False,  # Mudei para False para debug
        "no_warnings": False,
        "verbose": True,  # Ativa logs detalhados
        # Força re-encode de áudio se necessário
        "postprocessor_args": [
            "-c:v", "copy",  # Copia vídeo sem re-encode (mais rápido)
            "-c:a", "aac",   # Re-encode áudio para AAC (compatível com Twitter)
            "-b:a", "128k",  # Bitrate de áudio
        ],
        # Garante que o ffmpeg será usado para merge
        "prefer_ffmpeg": True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            logger.info(f"Iniciando download com yt-dlp: {url}")
            
            # Probe metadata first
            info = ydl.extract_info(url, download=False)
            duration = info.get("duration")
            
            # Log detalhado dos formatos disponíveis
            if "formats" in info:
                logger.info(f"Formatos disponíveis: {len(info['formats'])}")
                for fmt in info["formats"][:5]:  # Log primeiros 5 formatos
                    logger.info(f"  - Format: {fmt.get('format_id')} | "
                              f"Ext: {fmt.get('ext')} | "
                              f"Vcodec: {fmt.get('vcodec')} | "
                              f"Acodec: {fmt.get('acodec')}")

            # Verifica duração
            if duration and duration > 60:
                logger.info(f"Video duration {duration}s > 60s; marking as too long")
                return None, duration, "too_long"

            # Perform download com merge automático
            logger.info("Baixando e fazendo merge de vídeo+áudio...")
            ydl.download([url])

            if os.path.exists(output_filename):
                file_size = os.path.getsize(output_filename)
                logger.info(f"Download concluído! Arquivo: {output_filename} ({file_size} bytes)")
                
                # Verifica se o arquivo tem áudio usando ffprobe
                has_audio = check_audio_stream(output_filename)
                if not has_audio:
                    logger.warning("AVISO: Arquivo de vídeo NÃO contém stream de áudio!")
                else:
                    logger.info("✓ Áudio confirmado no arquivo de vídeo")
                
                return output_filename, duration, None
            else:
                return None, duration, "download_failed_no_file"

    except Exception as exc:
        logger.error(f"yt-dlp error for {url}: {exc}", exc_info=True)
        return None, None, str(exc)


def check_audio_stream(video_path: str) -> bool:
    """
    Verifica se o arquivo de vídeo contém um stream de áudio usando ffprobe.
    Retorna True se áudio existe, False caso contrário.
    """
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_type",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        has_audio = "audio" in result.stdout.lower()
        return has_audio
    except Exception as e:
        logger.warning(f"Não foi possível verificar áudio com ffprobe: {e}")
        return False  # Assume que não tem áudio se não conseguir verificar

# ---------- Twitter logic ----------

def _is_unrecoverable_tweepy_error(exc: tweepy.TweepyException) -> bool:
    """Return True if the error should NOT be retried."""
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

    if any(m in msg for m in fatal_markers):
        return True

    return False

def post_to_twitter(text: str, img_paths: list[str], video_path: str, post_id: str | None = None) -> tuple[bool, bool]:
    """
    Attempt to post to Twitter.
    Returns (success, fatal) where fatal=True means "don't retry / delete pending".
    Uses yt-dlp to fetch reddit video+audio merged when possible.
    """
    media_ids: list[int] = []

    try:
        # VIDEO HANDLING using yt-dlp
        if video_path:
            out_file = "temp_video.mp4"
            filename, duration, err = download_reddit_video_ytdlp(video_path, out_file)

            if err == "too_long":
                logger.info(f"Video too long (>60s). Will treat as fatal for post_id={post_id}")
                if post_id:
                    remove_pending_post(post_id)
                return False, True

            if filename is None:
                logger.error(f"YT-DLP download failed for {video_path}: {err}")
                if err and any(k in err.lower() for k in ("copyright", "404", "forbidden", "not permitted", "unavailable")):
                    if post_id:
                        remove_pending_post(post_id)
                    return False, True
                return False, False

            # Upload the final mp4
            try:
                check_rate_limits(api, "/media/upload")
                logger.info(f"Uploading video to Twitter: {filename}")
                media = api.media_upload(filename, media_category="tweet_video", chunked=True)
                media_ids.append(media.media_id)
                logger.info(f"✓ Video uploaded successfully! Media ID: {media.media_id}")
            except Exception as exc:
                logger.error(f"Error uploading video file {filename}: {exc}", exc_info=True)
                if post_id and _is_unrecoverable_tweepy_error(exc):
                    remove_pending_post(post_id)
                    return False, True
                return False, False
            finally:
                try:
                    if filename and os.path.exists(filename):
                        os.remove(filename)
                        logger.info(f"Cleaned up temp file: {filename}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup {filename}: {e}")

        # IMAGES
        elif img_paths:
            for idx, url in enumerate(img_paths[:4]):
                local = download_media(url, f"temp_image_{idx}.jpg")
                if local:
                    check_rate_limits(api, "/media/upload")
                    media = api.media_upload(local)
                    media_ids.append(media.media_id)
                    os.remove(local)

        # TWEET
        if text or media_ids:
            resp = client.create_tweet(
                text=text,
                media_ids=media_ids if media_ids else None,
                user_auth=True,
            )
            logger.info(f"✓ Tweet posted successfully: {resp.data['id']}")
            return True, False

        logger.error("Nothing to tweet: no text/media")
        return False, False

    except tweepy.TweepyException as exc:
        logger.error(f"Tweepy error: {exc}", exc_info=True)
        fatal = _is_unrecoverable_tweepy_error(exc)
        if fatal and post_id:
            remove_pending_post(post_id)
            logger.info(f"Dropped pending post {post_id} due to unrecoverable error: {exc}")
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

    for post in extractContent():
        if is_post_seen(post["id"]):
            continue

        img_paths: list[str] = []
        if post.get("s_img"):
            img_paths.append(post["s_img"])
        elif post.get("m_img"):
            img_paths.extend(post["m_img"])

        video_path = post.get("video", "")

        post_content = (
            post.get("title", "") + "\n" + post.get("content", "")
        ).strip()

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
            content.encode("utf-8", errors="replace").decode("utf-8")
            if content
            else ""
        )

        success, fatal = post_to_twitter(
            content, img_paths, video_path, post_id=post["id"]
        )

        if success:
            mark_post_as_seen(post["id"])
            return

        if fatal:
            logger.info(f"New post {post['id']} ignored permanently due to fatal error")
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
        sys.exit(1)

if __name__ == "__main__":
    main()