import sqlite3

database = 'seen_posts.db'

def get_db_connection(db: any):
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