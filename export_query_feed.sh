#!/bin/bash
# Script: get_newsboat_urls.sh
# Accepts multiple query terms, retrieves URLs for unread articles where title contains all terms (joined with AND), and appends to ~/urls.txt

# Output file in home directory
OUTPUT_FILE="$HOME/urls.txt"

# Check if at least one query term is provided
if [ $# -eq 0 ]; then
    echo "Error: Please provide at least one query term (e.g., bill clinton, hillary clinton)"
    exit 1
fi

# Validate query terms (basic sanitization to avoid SQL injection)
for term in "$@"; do
    if ! [[ "$term" =~ ^[a-zA-Z0-9_]+$ ]]; then
        echo "Error: Query term '$term' must contain only letters, numbers, or underscores"
        exit 1
    fi
done

# Check for cache.db
CACHE_DB="$HOME/.newsboat/cache.db"
if [ ! -f "$CACHE_DB" ]; then
    echo "Error: Cache database not found at $CACHE_DB"
    exit 1
fi

# Check for lock file
LOCK_FILE="$HOME/.newsboat/cache.db.lock"
if [ -f "$LOCK_FILE" ]; then
    echo "Warning: Database lock file found at $LOCK_FILE. Newsboat may be writing to the database."
    echo "Waiting up to 5 seconds for lock to clear..."
    sleep 5
    if [ -f "$LOCK_FILE" ]; then
        echo "Error: Lock file still present. Close Newsboat or wait and try again."
        exit 1
    fi
fi

# Ensure sqlite3 is installed
if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "Error: sqlite3 is not installed. Install it with: sudo apt install sqlite3"
    exit 1
fi

# Build SQL WHERE clause with AND conditions for each term
WHERE_CLAUSE="unread = 1"
QUERY_TERMS=("$@")
for i in "${!QUERY_TERMS[@]}"; do
    term="${QUERY_TERMS[$i]}"
    WHERE_CLAUSE="$WHERE_CLAUSE AND title LIKE '%$term%'"
done

# Extract URLs and count articles
echo "Extracting URLs for terms '${QUERY_TERMS[*]}'..."
URLS=$(sqlite3 "$CACHE_DB" "SELECT url FROM rss_item WHERE $WHERE_CLAUSE;")
ARTICLE_COUNT=$(sqlite3 "$CACHE_DB" "SELECT COUNT(*) FROM rss_item WHERE $WHERE_CLAUSE;")

if [ "$ARTICLE_COUNT" -eq 0 ]; then
    echo "No unread articles found for query '${QUERY_TERMS[*]}'. Try refreshing feeds with 'newsboat -r'."
    exit 1
fi

# Append URLs to file (creates file if it doesn't exist)
echo "Appending $ARTICLE_COUNT URLs to $OUTPUT_FILE..."
echo "$URLS" >> "$OUTPUT_FILE"

echo "Appended $ARTICLE_COUNT URLs for '${QUERY_TERMS[*]}' to $OUTPUT_FILE"
