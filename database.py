import sqlite3, logging, os

database = 'seen_posts.db'

def migrate_txt_to_db():
    """Migrate data from seen_posts.txt to the SQLite database"""
    try:
        # Check if the text file exists
        if not os.path.exists('seen_posts.txt'):
            logging.info("No seen_posts.txt file found to migrate")
            return
        
        logging.info("Starting migration from seen_posts.txt to SQLite database")
        
        # Read all post IDs from the text file
        with open('seen_posts.txt', 'r') as f:
            post_ids = [line.strip() for line in f if line.strip()]
        
        if not post_ids:
            logging.info("No posts found in seen_posts.txt")
            return
        
        conn = get_db_connection()
        try:
            # Insert all post IDs in a single transaction
            conn.executemany(
                'INSERT OR IGNORE INTO seen_posts (post_id) VALUES (?)',
                [(post_id,) for post_id in post_ids]
            )
            conn.commit()
            logging.info(f"Successfully migrated {len(post_ids)} posts to the database")
            
            # Optional: rename the old file to mark it as migrated
            os.rename('seen_posts.txt', 'seen_posts.txt.migrated')
            logging.info("Renamed seen_posts.txt to seen_posts.txt.migrated")
        except sqlite3.Error as e:
            logging.error(f"Database error during migration: {e}")
            conn.rollback()
        finally:
            conn.close()
    except Exception as e:
        logging.error(f"Error during migration: {e}")

def get_db_connection():
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    return conn

def is_post_seen(post_id):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM seen_posts WHERE post_id = ?', (post_id,))
        return cursor.fetchone() is not None
    finally:
        conn.close()

def mark_post_as_seen(post_id):
    conn = get_db_connection()
    try:
        conn.execute('INSERT OR IGNORE INTO seen_posts (post_id) VALUES (?)', (post_id,))
        conn.commit()
    finally:
        conn.close()