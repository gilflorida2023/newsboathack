#!/usr/bin/env python3
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
    def __init__(self, hosts):
        self.hosts = hosts
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.workers = []
        self.shutdown_flag = False
        
    def start(self):
        """Start worker threads"""
        for host in self.hosts:
            worker = threading.Thread(
                target=self._worker_loop,
                args=(host,),
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
    
    def _worker_loop(self, host):
        """Worker thread processing loop"""
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
        """Process a single URL using the specified host"""
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
                print(f"  [i] Processed in {processing_time:.1f}s on {host}: {url[:60]}...")
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
        """Add a URL processing task to the queue"""
        self.task_queue.put((url, callback))
    
    def shutdown(self):
        """Gracefully shutdown the worker pool"""
        self.shutdown_flag = True
        for _ in self.workers:
            self.task_queue.put(None)
        for worker in self.workers:
            worker.join(timeout=5)

class ActionDatabase:
    def __init__(self, db_path):
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
        """Create the queue and action tables if they don't exist"""
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
        
        self.conn.commit()
        
    def clear_queue(self):
        """Clear all records from the queue table"""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM queue")
        self.conn.commit()
        return cursor.rowcount
        
    def clear_actions(self):
        """Clear all records from the action table"""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM actions")
        self.conn.commit()
        return cursor.rowcount
        
    def count_queue(self):
        """Count records in the queue table"""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM queue")
        return cursor.fetchone()[0]
        
    def count_actions(self):
        """Count records in the action table"""
        if not self.conn:
            return 0
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM actions")
        return cursor.fetchone()[0]
        
    def add_to_queue(self, url, folder_name):
        """Add a URL to the queue table"""
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
        """Get all URLs from the queue table"""
        if not self.conn:
            return []
        cursor = self.conn.cursor()
        cursor.execute("SELECT url FROM queue")
        return [row[0] for row in cursor.fetchall()]
        
    def remove_from_queue(self, url):
        """Remove a URL from the queue table"""
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
        """Add a processed result to the action table"""
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
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

def determine_promotion(url, worker_pool):
    """Generate promotion using worker pool without timeouts"""
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
    """Gracefully stop Newsboat process if running"""
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
    """Remove control characters and normalize to ASCII"""
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
    """Check for and clear stale lock file if Newsboat is not running."""
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
    """Parse query entries from ~/.newsboat/urls, handling 'or' and parentheses."""
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
                # Skip lines that are just URLs (not queries)
                if line.startswith('http') or line.endswith('!'):
                    continue
                if not line:
                    continue
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
                         pass
                else:
                     pass
    except Exception as e:
        print(f"[ERROR] Error reading or processing file: {e}")
        sys.exit(1)
    return search_folders

def count_unread_articles(terms, logic):
    """Count unread articles for a search folder's terms and logic."""
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
    """Retrieve article URLs for a search folder's terms and logic."""
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
    """Get search folders with non-zero unread articles, sorted by count."""
    search_folders = parse_search_folders()
    nonzero_folders = []
    for folder in search_folders:
        count = count_unread_articles(folder["terms"], folder["logic"])
        if count > 0:
            nonzero_folders.append({"name": folder["name"], "terms": folder["terms"], "logic": folder["logic"], "count": count})
    return sorted(nonzero_folders, key=lambda x: x["count"], reverse=True)

def add_folders_to_queue(folders, db):
    """Add URLs from selected folders to the queue table"""
    if not db:
        print("Database connection not available")
        return

    # Get user selections
    selections = input("\nEnter folder numbers to add to queue (comma separated, or 'q' to quit): ").strip()
    if selections.lower() == 'q':
        return
    
    try:
        selected_indices = [int(s.strip())-1 for s in selections.split(',')]
        selected_folders = [folders[i] for i in selected_indices if 0 <= i < len(folders)]
        
        if not selected_folders:
            print("No valid folders selected")
            return

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
        print("Invalid input - please enter numbers separated by commas")

def process_queue(worker_pool, db):
    """Process URLs from queue table using AI workers"""
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
    # Process all URLs through worker pool
    with ThreadPoolExecutor(max_workers=len(worker_pool.hosts)) as executor:
        futures = {
            executor.submit(
                lambda url: (url, determine_promotion(url, worker_pool)),
                url
            ) for url in queue_urls
        }

        for future in as_completed(futures):
            url, (promotion, duration, host) = future.result()
            
            # Remove from queue regardless of success/failure
            if not db.remove_from_queue(url):
                print(f"  [!] Failed to remove from queue: {url[:60]}...")
            
            if promotion == "Error":
                print(f"  [!] Failed on {host}: {url[:80]}...")
                continue

            if db.add_to_actions(promotion, url):
                print(f"  [+] {host} ({duration:.1f}s): {promotion[:60]}...")
                total_processed += 1
            else:
                print(f"  [!] Failed to add to actions: {url[:60]}...")
                print(f"      Promotion length: {len(promotion)}")
                print(f"      Promotion preview: {promotion[:100]}...")

    print(f"\nProcessed {total_processed} URLs. Queue is now empty.")

def main():
    if len(sys.argv) < 2:
        print("Usage: ./tableintegrate.py <ollama_host1> [<ollama_host2> ...]")
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
        
        print("\nNewsboat URL Processor")
        print("=" * 50)
        print(f"Queue: {db.count_queue()} URLs")
        print(f"Actions: {db.count_actions()} processed URLs")
        
        while True:
            print("\nMain Menu:")
            print("[a] Add folders to queue")
            print("[p] Process queue with AI")
            print("[cq] Clear queue table")
            print("[ca] Clear actions table")
            print("[l] List search folders")
            print("[q] Quit")
            
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
        if was_running:
            # Start Newsboat in a way that doesn't block program exit
            try:
                subprocess.Popen(['newsboat'], 
                               start_new_session=True,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
                print("Newsboat restarted successfully")
            except Exception as e:
                print(f"Warning: Could not restart Newsboat - {str(e)}")

if __name__ == "__main__":
    main()
