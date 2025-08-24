#!/usr/bin/env python3
import sqlite3
import sys
import os
import re
import psutil
from pathlib import Path
from subprocess import run, PIPE, CalledProcessError, Popen, DEVNULL

# Newsboat database configuration
CACHE_DB = os.path.expanduser("~/.newsboat/cache.db")

class Clipboard:
    """Handles clipboard operations with pyclip-0.7.0"""
    @staticmethod
    def copy(text):
        try:
            import pyclip
            pyclip.copy(text)
            return True
        except (ImportError, Exception) as e:
            print(f"Clipboard error: {str(e)}")
            print("Text to paste manually:")
            print(text)
            return False

def is_newsboat_running():
    """Check if Newsboat is currently running"""
    for proc in psutil.process_iter(['name']):
        if proc.info['name'] == 'newsboat':
            return True
    return False

def stop_newsboat():
    """Gracefully stop Newsboat if running"""
    try:
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == 'newsboat':
                proc.terminate()
                proc.wait(timeout=5)
                return True
        return False
    except Exception as e:
        print(f"Warning: Could not stop Newsboat - {str(e)}")
        return False

def start_newsboat():
    """Start Newsboat in background"""
    try:
        Popen(['newsboat'],
             start_new_session=True,
             stdout=DEVNULL,
             stderr=DEVNULL)
        print("Newsboat restarted successfully")
        return True
    except Exception as e:
        print(f"Warning: Could not restart Newsboat - {str(e)}")
        return False

def validate_promotion(promotion, max_length=257):
    """Ensure promotion meets Twitter length requirements"""
    clean_text = re.sub(r'https?://\S+', '', promotion)
    return len(clean_text) <= max_length

def get_db_connection():
    """Connect to the Newsboat database"""
    return sqlite3.connect(CACHE_DB)

def ensure_actions_table(conn):
    """Ensure the actions table exists with correct schema"""
    cursor = conn.cursor()
    
    # Check if actions table exists and has the correct schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actions'")
    if cursor.fetchone():
        # Table exists, check its schema
        cursor.execute("PRAGMA table_info(actions)")
        columns = {col[1] for col in cursor.fetchall()}
        expected_columns = {"promotion", "url", "processed_at"}
        
        if columns != expected_columns:
            print("Actions table has wrong schema. Recreating...")
            cursor.execute("DROP TABLE actions")
            cursor.execute("""
            CREATE TABLE actions (
                promotion TEXT,
                url TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
    else:
        # Table doesn't exist, create it
        cursor.execute("""
        CREATE TABLE actions (
            promotion TEXT,
            url TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    
    conn.commit()

def get_next_tweet(conn):
    """Get the oldest unprocessed tweet from the actions table"""
    cursor = conn.cursor()
    cursor.execute("""
    SELECT promotion, url 
    FROM actions 
    ORDER BY processed_at ASC 
    LIMIT 1
    """)
    return cursor.fetchone()

def delete_processed_tweet(conn, url):
    """Remove processed tweet from the actions table"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM actions WHERE url = ?", (url,))
    conn.commit()

def process_tweets(max_length=257):
    """Main function to process tweets from database"""
    # Check and handle Newsboat state
    was_running = is_newsboat_running()
    if was_running:
        print("Newsboat is running - stopping it to access database...")
        if not stop_newsboat():
            print("Failed to stop Newsboat - database may be locked")
            return
    
    conn = None
    try:
        conn = get_db_connection()
        ensure_actions_table(conn)
        
        # Check if there are any actions to process
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM actions")
        action_count = cursor.fetchone()[0]
        
        if action_count == 0:
            print("No actions to process. Run tableintegrate.py first to process URLs.")
            return
        
        print(f"Found {action_count} actions to process")
        
        while True:
            tweet = get_next_tweet(conn)
            if not tweet:
                print("\n🏁 No more tweets to process!")
                break

            promotion, url = tweet
            if not promotion:
                print(f"\n⏩ Skipping empty promotion")
                print(f"URL: {url}")
                delete_processed_tweet(conn, url)
                continue
                
            promotion = promotion.strip()
            url = url.strip()
            
            if promotion == "REPEATED":
                print(f"\n⏩ Skipping repeated content")
                print(f"URL: {url}")
                delete_processed_tweet(conn, url)
                continue
            
            if not validate_promotion(promotion, max_length):
                print(f"\n⚠️ Skipping tweet (Exceeds {max_length} chars)")
                print(f"URL: {url}")
                delete_processed_tweet(conn, url)
                continue

            tweet_text = f"{promotion}\n{url}"
            print(f"\n✅ Tweet ready for posting:")
            print("="*40)
            print(tweet_text)
            print("="*40)
            print(f"Chars: {len(promotion)} (max {max_length}) + URL: {len(url)}")

            if not Clipboard.copy(tweet_text):
                print("\n⚠️ Could not copy to clipboard - please copy manually above")

            while True:
                action = input("\n[Enter] Post & Next  [s] Skip & Delete  [q] Quit: ").strip().lower()
                if action == 'q':
                    print("\n🏁 Finished posting!")
                    return
                elif action == 's':
                    print("Skipping and deleting...")
                    delete_processed_tweet(conn, url)
                    break
                elif not action:
                    delete_processed_tweet(conn, url)
                    print("Tweet processed and removed from database")
                    break
    except Exception as e:
        print(f"Error processing tweets: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Ensure database is closed and Newsboat is restarted
        if conn:
            conn.close()
        if was_running:
            start_newsboat()

if __name__ == "__main__":
    try:
        import psutil
    except ImportError:
        print("Error: psutil package required. Install with: pip install psutil")
        sys.exit(1)
        
    max_len = int(sys.argv[1]) if len(sys.argv) > 1 else 257
    process_tweets(max_len)
