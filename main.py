import tweepy
import schedule
import logging
import requests
import os
from time import sleep
from oauth import *
from reddit import *

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='twitter_bot.log'  # Save logs to a file
)

# Initialize Tweepy client (X API v2)
client = tweepy.Client(
    bearer_token=bearer_token,
    consumer_key=api_key,
    consumer_secret=api_secret,
    access_token=access_token,
    access_token_secret=access_token_secret
)

# Initialize Tweepy API (v1.1, for media upload)
auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

def download_media(url, filename):
    """Download media from a URL and save it locally."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        logging.info(f"Downloaded media from {url} to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Failed to download media from {url}: {e}")
        return None

def main():
    def postarX(text: str, img_paths: list, video_path: str):
        try:
            media_ids = []
            # Handle video
            if video_path:
                # Download the video from the URL
                local_filename = download_media(video_path, "temp_video.mp4")
                if local_filename:
                    # Upload the video
                    media = api.media_upload(local_filename, media_category="tweet_video", wait_for_processing=True)
                    media_ids.append(media.media_id)
                    # Clean up the downloaded file
                    os.remove(local_filename)
            
            # Handle multiple images (up to 4 per tweet)
            elif img_paths:
                for image_url in img_paths[:4]:  # Limit to 4 images
                    # Download the image from the URL
                    local_filename = download_media(image_url, "temp_image.jpg")
                    if local_filename:
                        # Upload the downloaded image
                        media = api.media_upload(local_filename)
                        media_ids.append(media.media_id)
                        # Clean up the downloaded file
                        os.remove(local_filename)

            # Post the tweet with text and/or media
            if text or media_ids:
                response = client.create_tweet(
                    text=text,  # Tweet text
                    media_ids=media_ids if media_ids else None  # Media IDs (if any)
                )
                logging.info(f"Posted content successfully. Tweet ID: {response.data['id']}")

                # Reset variables after posting
                img_paths.clear()
                video_path = ""
                text = ""

                # Update seen_posts.txt after posting
                with open("seen_posts.txt", "w") as f:
                    f.write("\n".join(seen_posts))
            else:
                logging.error("Failed to post content: No text or media provided.")
        except tweepy.TweepyException as e:
            logging.error(f"Failed to post content: {e}")

    # Schedule posting every hour
    schedule.every(1).hour.do(postarX)
    
    while True:
        try:
            # Initialize variables for each iteration
            img_paths = list()  # Ensure img_paths is always a list
            video_path = str()
            content = str()

            # Extract content from Reddit
            posts = extractContent()
            for post in posts:
                # Handle single image (s_img)
                if post.get('s_img'):
                    img_paths.append(post['s_img'])  # Add single image to the list
                # Handle multiple images (m_img)
                elif post.get('m_img'):
                    img_paths.extend(post['m_img'])  # Add multiple images to the list

                # Handle video
                video_path = post.get('video', '')

                # Ensure post['content'] and post['url'] are not None
                post_content = f"{post.get('title', '')}\n{post.get('content', '')}"
                post_url = post.get('url', '')

                if post_content is None or post_content == '':
                    post_content = post['title']
                if post_url is None:
                    post_url = ''

                # Log the values for debugging
                logging.info(f"Content: {post_content}")
                logging.info(f"Image Paths: {img_paths}")
                logging.info(f"Video Path: {video_path}")

                # Construct the content string
                if post_content and post_url:
                    content = f"{post_content[:(277 - len(post_url))]}...\n{post_url}" if len(post_content) + len(post_url) >= 277 else f"{post_content[:]}\n{post_url}"
                elif post_content:
                    content = f"{post_content[:277]}"
                elif post_url:
                    content = f"{post_url}"
                else:
                    logging.error("Both post_content and post_url are empty or None.")
                    continue

            # Post immediately after extracting content
            if content or img_paths or video_path:
                postarX(content, img_paths, video_path)

            # Run pending scheduled tasks
            schedule.run_pending()
            sleep(600)
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            sleep(60)

if __name__ == "__main__":
    main()