#!/usr/bin/env python3
"""
Newsboat URL Processor with AI Integration

This script processes RSS feed URLs from Newsboat using Ollama hosts
to generate AI-powered summaries and categorizations.

Usage:
    ./tableintegrate.py <ollama_host1> [<ollama_host2> ...]

Features:
- Processes unread articles from Newsboat search folders
- Uses multiple Ollama hosts for parallel processing
- Maintains a queue of URLs to process
- Stores processed results in a database
"""

import os
import sys
import sqlite3
from pathlib import Path
import re
import subprocess
import time
import psutil
import threading
import unicodedata
import shutil
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# Newsboat specific configuration
URLS_FILE = os.path.expanduser("~/.newsboat/urls")
CACHE_DB = os.path.expanduser("~/.newsboat/cache.db")
LOCK_FILE = os.path.expanduser("~/.newsboat/cache.db.lock")
BACKUP_DB = os.path.expanduser("~/.newsboat/cache.db.bak")

class OllamaWorkerPool:
    """A pool of workers for processing URLs using Ollama hosts."""
    
    def __init__(self, hosts):
        """
        Initialize the worker pool.
        
        Args:
            hosts (list): List of Ollama host addresses
        """
        self.hosts = hosts
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.workers = []
        self.shutdown_flag = False
        
    def start(self):
        """Start worker threads."""
        for host in self.hosts:
            worker = threading.Thread(
                target=self._worker_loop,
                args=(host,),
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
    
    def _worker_loop(self, host):
        """
        Worker thread processing loop.
        
        Args:
            host (str): The Ollama host this worker is connected to
        """
        while not self.shutdown_flag:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:  # Sentinel value for shutdown
                    break
                    
                url, callback = task
                try:
                    result = self._process_url(host, url)
                    self.result_queue.put((url, result))
                    if callback:
                        callback(url, result)
                except Exception as e:
                    print(f"Worker error on {host}: {str(e)}")
                finally:
                    self.task_queue.task_done()
            except:
                continue
    
    def _process_url(self, host, url):
        """
        Process a single URL using the specified host.
        
        Args:
            host (str): Ollama host address
            url (str): URL to process
            
        Returns:
            tuple: (promotion_text, processing_time, host)
        """
        try:
            start_time = time.time()
            result = subprocess.run(
                ["./article3.sh", host, url],
                capture_output=True,
                text=True,
                check=True,
                env={'TERM': 'dumb', **os.environ}
            )
            processing_time = time.time() - start_time
            if processing_time > 30:  # Log slow processing
                print(f"  [i] Processed in {processing_time:.1f}s on {host}: {url}")
            promotion = clean_text(result.stdout.strip())
            return (promotion if promotion and "Error" not in promotion else "Error", 
                    processing_time, 
                    host)
        except subprocess.CalledProcessError as e:
            print(f"  [!] Process error on {host}: {clean_text(e.stderr.strip())[:200]}...")
            return ("Error", 0, host)
        except Exception as e:
            print(f"  [!] System error on {host}: {str(e)}")
            return ("Error", 0, host)
    
    def add_task(self, url, callback=None):
        """
        Add a URL processing task to the queue.
        
        Args:
            url (str): URL to process
            callback (function): Optional callback function
        """
        self.task_queue.put((url, callback))
    
    def shutdown(self):
        """Gracefully shutdown the worker pool."""
        self.shutdown_flag = True
        for _ in self.workers:
            self.task_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)

class ActionDatabase:
    """Database handler for storing URLs and processing results."""
    
    def __init__(self, db_path):
        """
        Initialize the database connection.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            self.conn = sqlite3.connect(db_path)
            self._create_tables()
        except sqlite3.Error as e:
            print(f"Database error: {str(e)}")
            if self.conn:
                self.conn.close()
            raise
        
    def _create_tables(self):
        """Create the queue and action tables if they don't exist."""
        cursor = self.conn.cursor()
        
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
        
        # Create queue table (URLs to be processed)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            url TEXT PRIMARY KEY,
            folder_name TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS feed_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_url TEXT NOT NULL,
            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create defined_feeds table to store all feeds from urls file
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS defined_feeds (
            feed_url TEXT PRIMARY KEY,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        self.conn.commit()
        
    def clear_queue(self):
        """Clear all records from the queue table."""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM queue")
        self.conn.commit()
        return cursor.rowcount
        
    def clear_actions(self):
        """Clear all records from the action table."""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM actions")
        self.conn.commit()
        return cursor.rowcount
        
    def clear_feed_stats(self):
        """Clear all records from the feed_stats table."""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM feed_stats")
        self.conn.commit()
        return cursor.rowcount
        
    def count_queue(self):
        """Count records in the queue table."""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM queue")
        return cursor.fetchone()[0]
        
    def count_actions(self):
        """Count records in the action table."""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM actions")
        return cursor.fetchone()[0]
        
    def count_feed_stats(self):
        """Count records in the feed_stats table."""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM feed_stats")
        return cursor.fetchone()[0]

    def get_most_used_feeds(self, limit=10):
        """
        Get the most used RSS feeds from the feed_stats table.
        
        Args:
            limit (int): Maximum number of results to return
            
        Returns:
            list: List of tuples with (feed_url, usage_count)
        """
        if not self.conn:
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT feed_url, COUNT(*) as usage_count 
                FROM feed_stats 
                GROUP BY feed_url 
                ORDER BY usage_count DESC 
                LIMIT ?
            """, (limit,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"SQLite error in get_most_used_feeds: {e}")
            return []
    
    def get_least_used_feeds(self, limit=10):
        """
        Get the least used RSS feeds from the feed_stats table.
        
        Args:
            limit (int): Maximum number of results to return
            
        Returns:
            list: List of tuples with (feed_url, usage_count)
        """
        if not self.conn:
            return []
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT df.feed_url, COUNT(fs.feed_url) as usage_count
                FROM defined_feeds df
                LEFT JOIN feed_stats fs ON df.feed_url = fs.feed_url
                GROUP BY df.feed_url
                ORDER BY usage_count ASC, df.feed_url
                LIMIT ?
            """, (limit,))
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"SQLite error in get_least_used_feeds: {e}")
            return []
    
    def update_defined_feeds(self, feed_urls):
        """
        Update the defined_feeds table with current feed URLs.
        
        Args:
            feed_urls (list): List of feed URLs
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.conn:
            return False
        cursor = self.conn.cursor()
        try:
            # Clear existing defined feeds
            cursor.execute("DELETE FROM defined_feeds")
            
            # Insert current feeds
            for url in feed_urls:
                cursor.execute("""
                INSERT OR IGNORE INTO defined_feeds (feed_url)
                VALUES (?)
                """, (url,))
                
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"SQLite error in update_defined_feeds: {e}")
            return False
        
    def add_to_queue(self, url, folder_name):
        """
        Add a URL to the queue table.
        
        Args:
            url (str): URL to add to queue
            folder_name (str): Name of the folder this URL belongs to
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.conn:
            return False
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
            INSERT OR IGNORE INTO queue 
            (url, folder_name)
            VALUES (?, ?)
            """, (url, folder_name))
            self.conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"SQLite error in add_to_queue: {e}")
            return False
            
    def get_queue_urls(self):
        """Get all URLs from the queue table."""
        if not self.conn:
            return []
        cursor = self.conn.cursor()
        cursor.execute("SELECT url FROM queue")
        return [row[0] for row in cursor.fetchall()]
        
    def remove_from_queue(self, url):
        """
        Remove a URL from the queue table.
        
        Args:
            url (str): URL to remove from queue
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.conn:
            return False
        cursor = self.conn.cursor()
        try:
            cursor.execute("DELETE FROM queue WHERE url = ?", (url,))
            self.conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"SQLite error in remove_from_queue: {e}")
            return False
            
    def add_to_actions(self, promotion, url):
        """
        Add a processed result to the action table.
        
        Args:
            promotion (str): AI-generated promotion text
            url (str): URL that was processed
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.conn:
            return False
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
            INSERT OR REPLACE INTO actions 
            (promotion, url)
            VALUES (?, ?)
            """, (promotion, url))
            self.conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"SQLite error in add_to_actions: {e}")
            print(f"URL: {url}")
            print(f"Promotion length: {len(promotion)}")
            print(f"Promotion preview: {promotion[:100]}...")
            
            # If the error is about missing columns, recreate the table
            if "no such column" in str(e):
                print("Recreating actions table with correct schema...")
                self._create_tables()
                # Try again
                return self.add_to_actions(promotion, url)
                
            return False
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

def determine_promotion(url, worker_pool):
    """
    Generate promotion using worker pool without timeouts.
    
    Args:
        url (str): URL to process
        worker_pool (OllamaWorkerPool): Worker pool instance
        
    Returns:
        tuple: (promotion_text, processing_time, host)
    """
    result_queue = Queue()
    
    def callback(url, result):
        result_queue.put((url, result))
    
    worker_pool.add_task(url, callback)
    
    try:
        _, result = result_queue.get()  # No timeout
        return result
    except Exception as e:
        print(f"  [!] Queue error: {str(e)}")
        return ("Error", 0, "unknown")

def stop_newsboat():
    """
    Gracefully stop Newsboat process if running.
    
    Returns:
        bool: True if Newsboat was running, False otherwise
    """
    try:
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] == 'newsboat':
                print("Stopping Newsboat process...")
                proc.terminate()
                try:
                    proc.wait(5)
                except psutil.TimeoutExpired:
                    proc.kill()
                return True
        return False
    except Exception as e:
        print(f"Warning: Could not stop Newsboat - {str(e)}")
        return False

def clean_text(text):
    """
    Remove control characters and normalize to ASCII.
    
    Args:
        text (str): Text to clean
        
        Returns:
        str: Cleaned text
    """
    if not text:
        return ""
    text = re.sub(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])', '', text)
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)
    text = text.replace('"', "'").strip()
    text = re.sub(r'\s+', ' ', text)
    return text

def check_and_clear_lock_file():
    """
    Check for and clear stale lock file if Newsboat is not running.
    
    Returns:
        bool: True if lock file was cleared or doesn't exist, False otherwise
    """
    if not os.path.isfile(LOCK_FILE):
        return True
    try:
        result = subprocess.run(["pgrep", "-x", "newsboat"], capture_output=True, text=True)
        if result.stdout:
            print(f"Newsboat is running (PID: {result.stdout.strip()}). Terminating...")
            subprocess.run(["pkill", "-x", "newsboat"], check=True)
            time.sleep(1)
            result = subprocess.run(["pgrep", "-x", "newsboat"], capture_output=True, text=True)
            if result.stdout:
                print("Error: Failed to terminate Newsboat. Please close it manually.")
                return False
    except subprocess.CalledProcessError as e:
        print(f"Error checking Newsboat process: {e}")
        return False
    print(f"Stale lock file found at {LOCK_FILE}. Backing up database and removing lock...")
    try:
        if os.path.isfile(CACHE_DB):
            shutil.copy2(CACHE_DB, BACKUP_DB)
            print(f"Backed up database to {BACKUP_DB}")
        else:
            print(f"Error: Cache database not found at {CACHE_DB}")
            return False
        os.remove(LOCK_FILE)
        print(f"Removed stale lock file {LOCK_FILE}")
    except (OSError, IOError) as e:
        print(f"Error removing lock file or backing up database: {e}")
        return False
    return True

def parse_search_folders():
    """
    Parse query entries from ~/.newsboat/urls, handling 'or' and parentheses.
    
    Returns:
        list: List of search folder dictionaries with name, terms, and logic
    """
    if not os.path.exists(URLS_FILE):
        print(f"[ERROR] URLs file does not exist at {URLS_FILE}")
        sys.exit(1)
    if not os.path.isfile(URLS_FILE):
        print(f"[ERROR] Path {URLS_FILE} exists but is not a file")
        sys.exit(1)
    if not os.access(URLS_FILE, os.R_OK):
        print(f"[ERROR] URLs file at {URLS_FILE} is not readable")
        sys.exit(1)

    search_folders = []
    try:
        with open(URLS_FILE, 'r') as f:
            lines = f.readlines()
            if not lines:
                print("[ERROR] URLs file is empty")
                sys.exit(1)
            for i, line in enumerate(lines, 1):
                line = line.strip()
                # Skip empty lines
                if not line:
                    continue
                
                # Handle feed lines (remove trailing spaces and ! if present)
                if line.startswith('http'):
                    # Remove trailing spaces and ! if they exist
                    line = re.sub(r'\s*!\s*$', '', line)
                    continue
                
                # Handle quoted queries
                if line.startswith('"') and line.endswith('"'):
                    line = line[1:-1]
                
                if line.startswith("query:"):
                    match = re.match(r'^query:([^:]+):(.+)$', line)
                    if match:
                        name = match.group(1)
                        condition = match.group(2)
                        terms = []
                        logic = "AND"  # Default logic
                        if condition == 'unread = "yes"':
                            terms = ["unread"]
                        else:
                            # Check for OR within parentheses
                            or_match = re.search(r'\(\s*((?:title =~ \\"[^\\"]+\\"\s*(?:or\s*title =~ \\"[^\\"]+\\"\s*)*))\)', condition)
                            if or_match:
                                or_clause = or_match.group(1)
                                or_terms = re.findall(r'title =~ \\"([^\\"]+)\\"', or_clause)
                                terms = [term.lower() for term in or_terms]
                                logic = "OR"
                            else:
                                term_matches = re.findall(r'title =~ \\"([^\\"]+)\\"', condition)
                                for term in term_matches:
                                    terms.extend([t.lower() for t in term.split()])
                        search_folders.append({"name": name, "terms": terms, "logic": logic})
                    else:
                        print(f"[ERROR] Invalid query format on line {i}: {line}")
                #else:
                    #print(f"[WARNING] Skipping unrecognized line {i}: {line}")
    except Exception as e:
        print(f"[ERROR] Error reading or processing file: {e}")
        sys.exit(1)
    return search_folders

def parse_feed_urls():
    """
    Parse feed URLs from ~/.newsboat/urls file.
    
    Returns:
        list: List of feed URLs with trailing spaces and ! removed
    """
    if not os.path.exists(URLS_FILE):
        print(f"[ERROR] URLs file does not exist at {URLS_FILE}")
        sys.exit(1)
    if not os.path.isfile(URLS_FILE):
        print(f"[ERROR] Path {URLS_FILE} exists but is not a file")
        sys.exit(1)
    if not os.access(URLS_FILE, os.R_OK):
        print(f"[ERROR] URLs file at {URLS_FILE} is not readable")
        sys.exit(1)

    feed_urls = []
    try:
        with open(URLS_FILE, 'r') as f:
            lines = f.readlines()
            if not lines:
                print("[ERROR] URLs file is empty")
                sys.exit(1)
            for line in lines:
                line = line.strip()
                # Skip empty lines and query lines
                if not line or line.startswith("query:") or line.startswith('"'):
                    continue
                
                # Handle feed lines (remove trailing spaces and ! if present)
                if line.startswith('http'):
                    # Remove trailing spaces and ! if they exist
                    clean_url = re.sub(r'\s*!\s*$', '', line)
                    feed_urls.append(clean_url)
    except Exception as e:
        print(f"[ERROR] Error reading or processing file: {e}")
        sys.exit(1)
    return feed_urls

def get_feed_url_for_article(article_url):
    """
    Given an article URL, return the associated RSS feed URL from the Newsboat cache.

    Args:
        article_url (str): The article's URL.

    Returns:
        str or None: The RSS feed URL if found, else None.
    """
    if not check_and_clear_lock_file():
        return None

    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    feed_url = None
    try:
        # Directly get the feedurl from rss_item using the article URL
        cursor.execute("SELECT feedurl FROM rss_item WHERE url = ?", (article_url,))
        row = cursor.fetchone()
        if row:
            feed_url = row[0]
    except sqlite3.Error as e:
        print(f"SQLite error while getting feed URL: {e}")
    finally:
        conn.close()
    return feed_url

def view_feed_stats(db):
    """
    Display feed usage statistics.
    
    Args:
        db (ActionDatabase): Database instance
    """
    if not db or not db.conn:
        print("Database connection not available")
        return
        
    cursor = db.conn.cursor()
    try:
        cursor.execute("""
            SELECT feed_url, usage_count, date_added, last_used 
            FROM feed_stats 
            ORDER BY usage_count DESC, last_used DESC
        """)
        
        results = cursor.fetchall()
        
        if not results:
            print("No feed statistics available")
            return
            
        print("\nFeed Usage Statistics:")
        print("-" * 80)
        print(f"{'Feed URL':<40} {'Uses':<6} {'Added':<12} {'Last Used':<12}")
        print("-" * 80)
        
        for feed_url, usage_count, date_added, last_used in results:
            # Format dates for better readability
            added = date_added.split()[0] if date_added else "Unknown"
            used = last_used.split()[0] if last_used else "Never"
            
            # Truncate long URLs for display
            display_url = feed_url[:37] + "..." if len(feed_url) > 40 else feed_url
            
            print(f"{display_url:<40} {usage_count:<6} {added:<12} {used:<12}")
            
    except sqlite3.Error as e:
        print(f"SQLite error retrieving feed stats: {e}")

def update_feed_stats(db, rss_url):
    """
    Insert a record with the RSS URL and current timestamp into the feed stats table.
    
    Args:
        db (ActionDatabase): Database instance
        rss_url (str): RSS feed URL to record
        
    Returns:
        bool: True if successful, False otherwise
    """  
    if not db or not db.conn:
        print("Database connection not available")
        return False
    
    if not rss_url:
        print("No RSS URL provided")
        return False
    
    cursor = db.conn.cursor()
    try: 
        # Insert a new record with the RSS URL and current timestamp
        cursor.execute("""
            INSERT INTO feed_stats (feed_url, used_at)
            VALUES (?, CURRENT_TIMESTAMP)
        """, (rss_url,))
     
        db.conn.commit()
        #print(f"Added stats record for feed: {rss_url}")
        return True 
     
    except sqlite3.Error as e:
        print(f"SQLite error updating feed stats: {e}")
        db.conn.rollback()
        return False

def list_query_folders(verbose=True):
    """
    List all query folders from the Newsboat URLs file.
    Returns:
        list: List of query folder names
    """
    search_folders = parse_search_folders()
    
    if not search_folders:
        print("No query folders found")
        return []
    
    if verbose:
        print("\nQuery Folders:")
        for i, folder in enumerate(search_folders, 1):
            print(f"{i}. {folder['name']}")
    
    return [folder['name'] for folder in search_folders]

def list_folder_urls(folder_name,verbose=True):
    """
    List all URLs in a specific search folder.
    
    Args:
        folder_name (str): Name of the search folder to list URLs from       
    Returns:
        list: List of URLs in the specified folder
    """
    search_folders = parse_search_folders()
    
    # Find the folder with matching name
    target_folder = None
    for folder in search_folders:
        if folder["name"] == folder_name:
            target_folder = folder
            break
    
    if not target_folder:
        print(f"Folder '{folder_name}' not found")
        return []
    
    # Get URLs from the folder
    urls = get_article_urls(target_folder["terms"], target_folder["logic"])
    
    if verbose:
        print(f"\nURLs in folder '{folder_name}':")
        for i, url in enumerate(urls, 1):
            print(f"{i}. {url}")
    
    return urls

def update_usage_stats_for_all(db):
    query_folders = list_query_folders(False)
    for query_folder in query_folders:
        for url in list_folder_urls(query_folder,False):
            rssfeed = get_feed_url_for_article(url)
            update_feed_stats(db, rssfeed)

def count_unread_articles(terms, logic):
    """
    Count unread articles for a search folder's terms and logic.
    
    Args:
        terms (list): List of search terms
        logic (str): Logic to use ("AND" or "OR")
        
    Returns:
        int: Count of unread articles matching the criteria
    """
    if not check_and_clear_lock_file():
        return 0
        
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        if terms == ["unread"]:
            query = "SELECT COUNT(*) FROM rss_item WHERE unread = 1"
            cursor.execute(query)
        else:
            where_clause = "unread = 1"
            placeholders = [f'%{term}%' for term in terms]
            
            # Fix the SQL query to handle variable number of bindings
            if logic == "OR":
                # Create OR conditions for all terms
                or_conditions = " OR ".join(["title LIKE ?"] * len(terms))
                where_clause += f" AND ({or_conditions})"
            else:
                # Create AND conditions for all terms
                for _ in terms:
                    where_clause += " AND title LIKE ?"
            
            query = f"SELECT COUNT(*) FROM rss_item WHERE {where_clause}"
            cursor.execute(query, placeholders)
        count = cursor.fetchone()[0]
    except sqlite3.Error as e:
        print(f"SQLite error while counting articles: {e}")
        count = 0
    finally:
        conn.close()
    return count

def get_article_urls(terms, logic):
    """
    Retrieve article URLs for a search folder's terms and logic.
    
    Args:
        terms (list): List of search terms
        logic (str): Logic to use ("AND" or "OR")
        
    Returns:
        list: List of URLs matching the criteria
    """
    if not check_and_clear_lock_file():
        return []
        
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    urls = []
    try:
        if terms == ["unread"]:
            query = "SELECT url FROM rss_item WHERE unread = 1"
            cursor.execute(query)
        else:
            where_clause = "unread = 1"
            placeholders = [f'%{term}%' for term in terms]
            
            # Fix the SQL query to handle variable number of bindings
            if logic == "OR":
                # Create OR conditions for all terms
                or_conditions = " OR ".join(["title LIKE ?"] * len(terms))
                where_clause += f" AND ({or_conditions})"
            else:
                # Create AND conditions for all terms
                for _ in terms:
                    where_clause += " AND title LIKE ?"
            
            query = f"SELECT url FROM rss_item WHERE {where_clause}"
            cursor.execute(query, placeholders)
        urls = [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"SQLite error while getting URLs: {e}")
    finally:
        conn.close()
    return urls

def get_nonzero_folders():
    """
    Get search folders with non-zero unread articles, sorted by count.
    
    Returns:
        list: List of folders with unread articles, sorted by count
    """
    search_folders = parse_search_folders()
    nonzero_folders = []
    for folder in search_folders:
        count = count_unread_articles(folder["terms"], folder["logic"])
        if count > 0:
            nonzero_folders.append({"name": folder["name"], "terms": folder["terms"], "logic": folder["logic"], "count": count})
    return sorted(nonzero_folders, key=lambda x: x["count"], reverse=True)

#def add_folders_to_queue(folders, db):
#    """
#    Add URLs from selected folders to the queue table.
#    
#    Args:
#        folders (list): List of folder dictionaries
#        db (ActionDatabase): Database instance
#    """
#    if not db:
#        print("Database connection not available")
#        return
#
#    # Get user selections
#    selections = input("\nEnter folder numbers to add to queue (comma separated, or 'q' to quit): ").strip()
#    if selections.lower() == 'q':
#        return
#    
#    try:
#        selected_indices = [int(s.strip())-1 for s in selections.split(',')]
#        selected_folders = [folders[i] for i in selected_indices if 0 <= i < len(folders)]
#        
#        if not selected_folders:
#            print("No valid folders selected")
#            return
#
#        # Gather ALL URLs from selected folders and add to queue
#        total_added = 0
#        for folder in selected_folders:
#            urls = get_article_urls(folder['terms'], folder['logic'])
#            if urls:
#                for url in urls:
#                    if db.add_to_queue(url, folder['name']):
#                        total_added += 1
#        
#        print(f"\nAdded {total_added} URLs to queue from {len(selected_folders)} folders.")
#        
#    except ValueError:
#        print("Invalid input - please enter numbers separated by commas")

def add_folders_to_queue(folders, db):
    """
    Add URLs from selected folders to the queue table.
    
    Args:
        folders (list): List of folder dictionaries
        db (ActionDatabase): Database instance
    """
    if not db:
        print("Database connection not available")
        return

    # Get user selections
    selections = input("\nEnter folder numbers to add to queue (comma separated, ranges with '-', or 'q' to quit): ").strip()
    if selections.lower() == 'q':
        return
    
    try:
        # Parse ranges and individual selections
        selected_indices = []
        for part in selections.split(','):
            part = part.strip()
            if '-' in part:
                # Handle range (e.g., "1-6")
                start, end = map(int, part.split('-'))
                selected_indices.extend(range(start-1, end))
            elif ':' in part:
                # Handle range with colon (e.g., "1:6")
                start, end = map(int, part.split(':'))
                selected_indices.extend(range(start-1, end))
            else:
                # Handle individual number
                selected_indices.append(int(part)-1)
        
        # Remove duplicates and sort
        selected_indices = sorted(set(selected_indices))
        
        # Filter valid indices
        selected_indices = [i for i in selected_indices if 0 <= i < len(folders)]
        
        if not selected_indices:
            print("No valid folders selected")
            return

        selected_folders = [folders[i] for i in selected_indices]
        
        # Gather ALL URLs from selected folders and add to queue
        total_added = 0
        for folder in selected_folders:
            urls = get_article_urls(folder['terms'], folder['logic'])
            if urls:
                for url in urls:
                    if db.add_to_queue(url, folder['name']):
                        total_added += 1
        
        print(f"\nAdded {total_added} URLs to queue from {len(selected_folders)} folders.")
        
    except ValueError:
        print("Invalid input - please enter numbers separated by commas, or ranges like 1-6")


def process_queue(worker_pool, db):
    """
    Process URLs from queue table using AI workers.
    
    Args:
        worker_pool (OllamaWorkerPool): Worker pool instance
        db (ActionDatabase): Database instance
    """
    if not db:
        print("Database connection not available")
        return
        
    queue_count = db.count_queue()
    if queue_count == 0:
        print("Queue is empty. Add URLs to queue first.")
        return
        
    print(f"\nProcessing {queue_count} URLs from queue...")
    
    # Get all URLs from queue
    queue_urls = db.get_queue_urls()
    
    total_processed = 0
    current_item = 0
    
    # Process all URLs through worker pool
    with ThreadPoolExecutor(max_workers=len(worker_pool.hosts)) as executor:
        futures = {
            executor.submit(
                lambda url: (url, determine_promotion(url, worker_pool)),
                url
            ) for url in queue_urls
        }

        for future in as_completed(futures):
            current_item += 1
            url, (promotion, duration, host) = future.result()
            
            # Remove from queue regardless of success/failure
            if not db.remove_from_queue(url):
                print(f"  [{current_item}/{queue_count}] Failed to remove from queue: {url}")
            
            if promotion == "Error":
                print(f"  [{current_item}/{queue_count}] Failed on {host}: {url}")
                continue

            if db.add_to_actions(promotion, url):
                #print(f"  [{current_item}/{queue_count}] {host} ({duration:.1f}s): {promotion[:60]}...")
                rssfeed = get_feed_url_for_article(url)
                if rssfeed is None:
                    rssfeed = "unknown"

                print(f"  [{current_item}/{queue_count}] {rssfeed} ({duration:.1f}s): {promotion[:60]}...")
                total_processed += 1
            else:
                print(f"  [{current_item}/{queue_count}] Failed to add to actions: {url}")
                print(f"      Promotion length: {len(promotion)}")
                print(f"      Promotion preview: {promotion[:100]}...")

    print(f"\nProcessed {total_processed} URLs. Queue is now empty.")
    
def print_main_menu():
    """Print the main menu options without the usage text."""
    print("\nMain Menu:")
    print("Available commands:")
    print("  [a] Add folders to queue")
    print("  [p] Process queue with AI")
    print("  [cq] Clear queue table")
    print("  [ca] Clear actions table")
    print("  [cs] Clear feed stats")
    print("  [most] Show most used feeds")
    print("  [least] Show least used feeds")
    print("  [l] List search folders")
    print("  [q] Quit")

def main():
    """Main function to run the URL processor."""
    
    # The help text from the docstring is moved here
    HELP_TEXT = """Newsboat URL Processor with AI Integration

This script processes RSS feed URLs from Newsboat using Ollama hosts
to generate AI-powered summaries and categorizations.

Usage:
    ./tableintegrate.py <ollama_host1> [<ollama_host2> ...]

Features:
- Processes unread articles from Newsboat search folders
- Uses multiple Ollama hosts for parallel processing
- Maintains a queue of URLs to process
- Stores processed results in a database"""
    
    if len(sys.argv) < 2:
        print("Error: No Ollama hosts specified")
        print(HELP_TEXT)
        sys.exit(1)
    
    ollama_hosts = sys.argv[1:]
    print(f"Using Ollama hosts: {', '.join(ollama_hosts)}")
    
    worker_pool = OllamaWorkerPool(ollama_hosts)
    worker_pool.start()
    
    was_running = stop_newsboat()
    
    db = None
    try:
        if not os.path.isfile(CACHE_DB):
            print(f"Error: Newsboat cache database not found at {CACHE_DB}")
            print("Please make sure Newsboat is installed and has been run at least once.")
            sys.exit(1)
            
        db = ActionDatabase(CACHE_DB)
        
        # Parse feed URLs and update the defined_feeds table
        feed_urls = parse_feed_urls()
        db.update_defined_feeds(feed_urls)
        
        update_usage_stats_for_all(db)
        
        while True:
            print("\nNewsboat URL Processor")
            print("=" * 50)
            print(f"{'Queue:':14s} {db.count_queue()} URLs")
            print(f"{'Action:':14s} {db.count_actions()} processed URLs")
            print(f"{'Feed_stats:':14s} {db.count_feed_stats()}" )
            print_main_menu()
            
            selection = input("\nEnter selection: ").strip().lower()
            
            if selection == 'q':
                break
                
            elif selection == 'cq':
                count = db.clear_queue()
                print(f"\nCleared queue table (removed {count} URLs)")
                print(f"Queue: {db.count_queue()} URLs")
                
            elif selection == 'ca':
                count = db.clear_actions()
                print(f"\nCleared actions table (removed {count} records)")
                print(f"Actions: {db.count_actions()} processed URLs")
                
            elif selection == 'cs':
                count = db.clear_feed_stats()
                print(f"\nCleared feed stats table (removed {count} records)")
                
            elif selection == 'most':
                most_used = db.get_most_used_feeds(20)  # Show top 20
                if not most_used:
                    print("No feed statistics available")
                    continue
                    
                print("\nMost Used RSS Feeds:")
                print("-" * 80)
                print(f"{'Rank':<5} {'Feed URL':<60} {'Uses':<6}")
                print("-" * 80)
                
                for i, (feed_url, usage_count) in enumerate(most_used, 1):
                    # Truncate long URLs for display
                    display_url = feed_url[:57] + "..." if len(feed_url) > 60 else feed_url
                    print(f"{i:<5} {display_url:<60} {usage_count:<6}")
            
            elif selection == 'least':
                least_used = db.get_least_used_feeds(1000)
                if not least_used:
                    print("No feed statistics available")
                    continue
                    
                print("\nLeast Used RSS Feeds:")
                print("-" * 80)
                print(f"{'Rank':<5} {'Feed URL':<60} {'Uses':<6}")
                print("-" * 80)
                
                for i, (feed_url, usage_count) in enumerate(least_used, 1):
                    # Truncate long URLs for display
                    display_url = feed_url[:57] + "..." if len(feed_url) > 60 else feed_url
                    print(f"{i:<5} {display_url:<60} {usage_count:<6}")
                
            elif selection == 'l':
                print("\nAvailable Search Folders:")
                folders = get_nonzero_folders()
                if not folders:
                    print("No search folders with unread articles found")
                    continue
                    
                for i, folder in enumerate(folders, 1):
                    print(f"{i}. {folder['name']} ({folder['count']} items)")
                    
            elif selection == 'a':
                folders = get_nonzero_folders()
                if not folders:
                    print("No search folders with unread articles found")
                    continue
                    
                print("\nFolders with unread articles:")
                for i, folder in enumerate(folders, 1):
                    print(f"{i}. {folder['name']} ({folder['count']} items)")
                
                add_folders_to_queue(folders, db)
                print(f"Queue: {db.count_queue()} URLs")
                    
            elif selection == 'p':
                process_queue(worker_pool, db)
                print(f"Actions: {db.count_actions()} processed URLs")
                    
            else:
                print("Invalid selection. Please try again.")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        worker_pool.shutdown()
        if db:
            db.close()

if __name__ == "__main__":
    main()
