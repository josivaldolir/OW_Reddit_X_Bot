import tweepy, logging, requests, os, time, subprocess, sqlite3
from logging.handlers import RotatingFileHandler
from oauth import *
from reddit import *
from database import migrate_txt_to_db, get_db_connection

# Configure rotating logs (max 5 files, 1MB each)
log_handler = RotatingFileHandler(
    'twitter_bot.log',
    maxBytes=1_000_000,  # 1MB per file
    backupCount=5,       # Keep 5 old logs
    encoding='utf-8'
)

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='twitter_bot.log',
    handlers=[log_handler]
)

# Initialize Tweepy clients
client = tweepy.Client(
    bearer_token=bearer_token,
    consumer_key=api_key,
    consumer_secret=api_secret,
    access_token=access_token,
    access_token_secret=access_token_secret
)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# Database setup
database = 'seen_posts.db'

def initialize_db():
    conn = get_db_connection()
    conn.execute('''CREATE TABLE IF NOT EXISTS pending_posts 
                   (post_id TEXT PRIMARY KEY, 
                    content TEXT, 
                    img_paths TEXT,  # JSON array
                    video_path TEXT,
                    attempts INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.execute('CREATE TABLE IF NOT EXISTS seen_posts (post_id TEXT PRIMARY KEY)')
    conn.commit()
    conn.close()
    
    migrate_txt_to_db()

def is_post_seen(post_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM seen_posts WHERE post_id = ?', (post_id,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def mark_post_as_seen(post_id):
    conn = get_db_connection()
    try:
        conn.execute('INSERT INTO seen_posts (post_id) VALUES (?)', (post_id,))
        conn.execute('DELETE FROM pending_posts WHERE post_id = ?', (post_id,))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # Post already exists
    finally:
        conn.close()

def save_pending_post(post_id, content, img_paths, video_path):
    conn = get_db_connection()
    try:
        # Convert img_paths list to JSON string
        img_paths_json = '[]' if not img_paths else f'[{",".join(f"""\"{img}\"""" for img in img_paths)}]'
        
        conn.execute('''INSERT OR REPLACE INTO pending_posts 
                       (post_id, content, img_paths, video_path, attempts, last_attempt) 
                       VALUES (?, ?, ?, ?, COALESCE((SELECT attempts+1 FROM pending_posts WHERE post_id = ?), CURRENT_TIMESTAMP)''',
                    (post_id, content, img_paths_json, video_path, post_id))
        conn.commit()
    finally:
        conn.close()

def get_pending_posts():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''SELECT post_id, content, img_paths, video_path 
                         FROM pending_posts 
                         WHERE attempts < 3 
                         ORDER BY last_attempt ASC''')
        posts = []
        for row in cursor.fetchall():
            # Convert JSON string back to list
            img_paths = [] if row['img_paths'] == '[]' else [img.strip('"') for img in row['img_paths'][1:-1].split(',')]
            posts.append({
                'post_id': row['post_id'],
                'content': row['content'],
                'img_paths': img_paths,
                'video_path': row['video_path']
            })
        return posts
    finally:
        conn.close()

def download_media(url, filename):
    """Download media from a URL and save it locally."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logging.info(f"Downloaded media from {url} to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Failed to download media from {url}: {e}")
        return None

def combine_video_audio(video_path, audio_path, output_path):
    """Combine video and audio files using ffmpeg."""
    try:
        command = [
            'ffmpeg',
            '-i', video_path,
            '-i', audio_path,
            '-c:v', 'copy',
            '-c:a', 'aac',
            '-strict', 'experimental',
            output_path
        ]
        subprocess.run(command, check=True)
        logging.info(f"Combined video and audio into {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to combine video and audio: {e}")
        return None

def check_rate_limits(api, endpoint):
    """Check rate limits for a specific endpoint."""
    try:
        rate_limit_status = api.rate_limit_status()
        endpoint_limit = rate_limit_status['resources'][endpoint.split('/')[1]][endpoint]
        
        if endpoint_limit['remaining'] <= 10:
            reset_time = endpoint_limit['reset']
            sleep_time = reset_time - time.time()
            if sleep_time > 0:
                logging.info(f"Approaching rate limit. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)
    except tweepy.TweepyException as e:
        logging.error(f"Failed to check rate limits: {e}")

def post_to_twitter(text: str, img_paths: list, video_path: str):
    """Attempt to post to Twitter and return success status"""
    try:
        media_ids = []
        
        # Handle video
        if video_path:
            video_url = video_path
            audio_url = video_url.replace('DASH_720.mp4', 'DASH_AUDIO_128.mp4')
            
            video_filename = download_media(video_url, "temp_video.mp4")
            audio_filename = download_media(audio_url, "temp_audio.mp4")

            if video_filename and audio_filename:
                combined_filename = "temp_combined.mp4"
                if combine_video_audio(video_filename, audio_filename, combined_filename):
                    check_rate_limits(api, '/media/upload')
                    media = api.media_upload(combined_filename, media_category="tweet_video")
                    media_ids.append(media.media_id)
                    os.remove(video_filename)
                    os.remove(audio_filename)
                    os.remove(combined_filename)
        
        # Handle images
        elif img_paths:
            for image_url in img_paths[:4]:
                local_filename = download_media(image_url, "temp_image.jpg")
                if local_filename:
                    check_rate_limits(api, '/media/upload')
                    media = api.media_upload(local_filename)
                    media_ids.append(media.media_id)
                    os.remove(local_filename)

        # Post the tweet
        if text or media_ids:
            check_rate_limits(api, '/tweets&POST')
            response = client.create_tweet(
                text=text,
                media_ids=media_ids if media_ids else None,
                user_auth=True
            )
            logging.info(f"Posted content successfully. Tweet ID: {response.data['id']}")
            return True
            
        logging.error("Failed to post content: No text or media provided.")
        return False
        
    except tweepy.TweepyException as e:
        logging.error(f"Failed to post content: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error in post_to_twitter: {e}")
        return False

def process_posts():
    """Process both new and pending posts"""
    try:
        # First try pending posts
        pending_posts = get_pending_posts()
        for post in pending_posts:
            success = post_to_twitter(
                post['content'],
                post['img_paths'],
                post['video_path']
            )
            if success:
                mark_post_as_seen(post['post_id'])
            else:
                logging.info(f"Post {post['post_id']} failed, will retry later")

        # Then process new posts
        posts = extractContent()
        for post in posts:
            if is_post_seen(post['id']):
                continue

            img_paths = []
            if post.get('s_img'):
                img_paths.append(post['s_img'])
            elif post.get('m_img'):
                img_paths.extend(post['m_img'])

            video_path = post.get('video', '')

            post_content = f"{post.get('title', '')}\n{post.get('content', '')}"
            post_url = post.get('url', '')

            if post_content is None or post_content == '':
                post_content = post['title']
            if post_url is None:
                post_url = ''

            if post_content and post_url:
                content = f"{post_content[:(277 - len(post_url))]}...\n{post_url}" if len(post_content) + len(post_url) >= 277 else f"{post_content[:]}\n{post_url}"
            elif post_content:
                content = f"{post_content[:277]}"
            elif post_url:
                content = f"{post_url}"
            else:
                logging.error("Both post_content and post_url are empty or None.")
                continue

            success = post_to_twitter(content, img_paths, video_path)
            if success:
                mark_post_as_seen(post['id'])
            else:
                save_pending_post(post['id'], content, img_paths, video_path)
                logging.info(f"Saved post {post['id']} for retry")

    except Exception as e:
        logging.error(f"Error in process_posts: {e}")

def main():
    initialize_db()
    while True:
        try:
            process_posts()
            # Wait before next check (e.g., 5 minutes)
            time.sleep(300)
        except KeyboardInterrupt:
            logging.info("Bot stopped by user")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()