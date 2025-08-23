#!/usr/bin/env python3
# Script: newsboat_search_menu.py
# Parses search folders from ~/.newsboat/urls once, supports 'or' and parentheses, shows a menu of non-zero unread article folders sorted by count,
# and appends selected folder URLs to a unique 'actions' table in ~/.newsboat/cache.db

import sqlite3
import os
import re
import sys
import subprocess
import time
import shutil

# File paths
URLS_FILE = os.path.expanduser("~/.newsboat/urls")
CACHE_DB = os.path.expanduser("~/.newsboat/cache.db")
LOCK_FILE = os.path.expanduser("~/.newsboat/cache.db.lock")
BACKUP_DB = os.path.expanduser("~/.newsboat/cache.db.bak")

def check_and_terminate_newsboat():
    """Check if Newsboat is running and terminate it."""
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
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error checking/terminating Newsboat: {e}")
        return False

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
    if not os.path.isfile(URLS_FILE):
        print(f"Error: URLs file not found at {URLS_FILE}")
        sys.exit(1)
    
    search_folders = []
    with open(URLS_FILE, 'r') as f:
        for line in f:
            line = line.strip()
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
                        # Check for OR within parentheses, e.g., (title =~ "starv" or title =~ "famine")
                        or_match = re.search(r'\(\s*title =~ \\"([^\\"]+)\\"\s+or\s+title =~ \\"([^\\"]+)\\"\s*\)', condition)
                        if or_match:
                            terms = [or_match.group(1).lower(), or_match.group(2).lower()]
                            logic = "OR"
                        else:
                            # Standard AND terms
                            term_matches = re.findall(r'title =~ \\"([^\\"]+)\\"', condition)
                            for term in term_matches:
                                terms.extend([t.lower() for t in term.split()])
                    print(f"Added folder: {name} with terms {terms}, logic {logic}")
                    search_folders.append({"name": name, "terms": terms, "logic": logic})
                else:
                    print(f"Warning: Failed to parse query line: {line}")
    return search_folders

def count_unread_articles(terms, logic):
    """Count unread articles for a search folder's terms and logic."""
    if not check_and_clear_lock_file():
        sys.exit(1)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        if terms == ["unread"]:
            query = "SELECT COUNT(*) FROM rss_item WHERE unread = 1"
            cursor.execute(query)
        else:
            where_clause = "unread = 1"
            placeholders = [f'%{term}%' for term in terms]
            if logic == "OR":
                where_clause += f" AND (title LIKE ? OR title LIKE ?)"
            else:
                for i in range(len(terms)):
                    where_clause += f" AND title LIKE ?"
            query = f"SELECT COUNT(*) FROM rss_item WHERE {where_clause}"
            cursor.execute(query, placeholders)
        count = cursor.fetchone()[0]
        print(f"Count for terms {terms}, logic {logic}: {count}")
    except sqlite3.Error as e:
        print(f"SQLite error while counting articles for terms {terms}, logic {logic}: {e}")
        count = 0
    finally:
        conn.close()
    return count

def get_nonzero_folders():
    """Get search folders with non-zero unread articles, sorted by count."""
    search_folders = parse_search_folders()
    nonzero_folders = []
    for folder in search_folders:
        count = count_unread_articles(folder["terms"], folder["logic"])
        if count > 0:
            nonzero_folders.append({"name": folder["name"], "terms": folder["terms"], "logic": folder["logic"], "count": count})
            print(f"Non-zero folder: {folder['name']} ({count})")
    return sorted(nonzero_folders, key=lambda x: x["count"], reverse=True)

def create_actions_table():
    """Create the actions table if it doesn't exist."""
    if not check_and_clear_lock_file():
        sys.exit(1)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS actions (
                url TEXT PRIMARY KEY
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error while creating actions table: {e}")
        sys.exit(1)
    finally:
        conn.close()

def get_actions_count():
    """Count URLs in the actions table."""
    if not check_and_clear_lock_file():
        sys.exit(1)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM actions")
        count = cursor.fetchone()[0]
    except sqlite3.Error as e:
        print(f"SQLite error while counting actions: {e}")
        count = 0
    finally:
        conn.close()
    return count

def append_urls_to_actions(terms, logic):
    """Append URLs for a search folder's terms and logic to the actions table."""
    if not check_and_clear_lock_file():
        sys.exit(1)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        if terms == ["unread"]:
            query = "SELECT url FROM rss_item WHERE unread = 1"
            cursor.execute(query)
        else:
            where_clause = "unread = 1"
            placeholders = [f'%{term}%' for term in terms]
            if logic == "OR":
                where_clause += f" AND (title LIKE ? OR title LIKE ?)"
            else:
                for i in range(len(terms)):
                    where_clause += f" AND title LIKE ?"
            query = f"SELECT url FROM rss_item WHERE {where_clause}"
            cursor.execute(query, placeholders)
        urls = [row[0] for row in cursor.fetchall()]
        for url in urls:
            cursor.execute("INSERT OR IGNORE INTO actions (url) VALUES (?)", (url,))
        conn.commit()
        print(f"Appended {len(urls)} URLs for terms {terms}, logic {logic}")
    except sqlite3.Error as e:
        print(f"SQLite error while appending URLs for terms {terms}, logic {logic}: {e}")
        urls = []
    finally:
        conn.close()
    return len(urls)

def clear_actions_table():
    """Clear the actions table and recreate it."""
    if not check_and_clear_lock_file():
        sys.exit(1)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS actions")
        cursor.execute("CREATE TABLE actions (url TEXT PRIMARY KEY)")
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite error while clearing actions table: {e}")
    finally:
        conn.close()

def display_actions_table():
    """Display all URLs in the actions table."""
    if not check_and_clear_lock_file():
        sys.exit(1)
    conn = sqlite3.connect(CACHE_DB)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT url FROM actions")
        urls = [row[0] for row in cursor.fetchall()]
        if not urls:
            print("Actions table is empty.")
        else:
            print("URLs in actions table:")
            for url in urls:
                print(url)
    except sqlite3.Error as e:
        print(f"SQLite error while displaying actions table: {e}")
    finally:
        conn.close()

def main():
    """Interactive menu for selecting search folders and managing actions table."""
    if not os.path.isfile(CACHE_DB):
        print(f"Error: Cache database not found at {CACHE_DB}")
        sys.exit(1)
    
    create_actions_table()
    
    # Parse and count once at startup
    print("Parsing search folders and counting unread articles...")
    nonzero_folders = get_nonzero_folders()
    print("Parsing complete.")
    
    while True:
        actions_count = get_actions_count()
        
        print("\nNewsboat Search Folder Menu")
        print(f"URLs in actions table: {actions_count}")
        print("Options:")
        print("  q - Quit")
        print("  c - Clear actions table")
        print("  d - Display actions table")
        if nonzero_folders:
            print("Non-zero search folders (sorted by unread count):")
            for i, folder in enumerate(nonzero_folders, 1):
                print(f"  {i}. {folder['name']} ({folder['count']})")
        else:
            print("No non-zero search folders found.")
        
        choice = input("Select an option: ").strip().lower()
        
        if choice == 'q':
            print("Exiting.")
            break
        elif choice == 'c':
            clear_actions_table()
            print("Actions table cleared.")
        elif choice == 'd':
            display_actions_table()
        elif choice.isdigit() and 1 <= int(choice) <= len(nonzero_folders):
            folder = nonzero_folders[int(choice) - 1]
            count = append_urls_to_actions(folder["terms"], folder["logic"])
            print(f"Appended {count} URLs for '{folder['name']}' to actions table.")
        else:
            print("Invalid choice. Please select a valid option.")

if __name__ == "__main__":
    main()
