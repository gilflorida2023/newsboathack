# newsboat_db_dumper.py
# A utility to dump key article information from the Newsboat cache database.

import sqlite3
import os
import sys

def dump_feed_data():
    """
    Connects to the Newsboat cache database, prints the table schemas,
    and then dumps a list of articles with their corresponding feed URLs.
    """
    # Define the path to the Newsboat cache database.
    db_path = os.path.expanduser("~/.newsboat/cache.db")

    # Check if the database file exists before attempting to connect.
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at '{db_path}'")
        print("Please ensure Newsboat has been run at least once.")
        sys.exit(1)

    try:
        # Connect to the SQLite database.
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Step 1: Dump the schema for the key tables.
        # This will show the actual column names used by Newsboat.
        print("-" * 80)
        print("Dumping table schemas...")
        print("-" * 80)

        # Get the schema for the rss_item table.
        cursor.execute("PRAGMA table_info(rss_item);")
        rss_item_schema = cursor.fetchall()
        print("Schema for 'rss_item' table:")
        for col_info in rss_item_schema:
            print(f"  - Column: {col_info[1]}, Type: {col_info[2]}")

        # Get the schema for the rss_feed table.
        cursor.execute("PRAGMA table_info(rss_feed);")
        rss_feed_schema = cursor.fetchall()
        print("\nSchema for 'rss_feed' table:")
        for col_info in rss_feed_schema:
            print(f"  - Column: {col_info[1]}, Type: {col_info[2]}")
        print("-" * 80)

        # Step 2: Perform the data dump with the corrected query.
        print("\nDumping article and feed data...")
        print("-" * 80)

        # The core SQL query to join the rss_item and rss_feed tables.
        # We join on the 'feedurl' column from the rss_item table and the
        # 'url' column from the rss_feed table.
        # We select the article title, article URL, and the feed URL.
        # The 'url' column is now used for the article's link from rss_item.
        query = """
        SELECT
            T1.title AS article_title,
            T1.url AS article_url,
            T2.url AS feed_url
        FROM
            rss_item AS T1
        JOIN
            rss_feed AS T2 ON T1.feedurl = T2.url;
        """

        # Execute the query.
        cursor.execute(query)

        # Fetch all the results from the query.
        rows = cursor.fetchall()

        # Print the header for the output.
        print(f"{'Article Title':<40} | {'Article URL':<40} | {'Feed URL':<40}")
        print("-" * 80)

        # Iterate through the rows and print the data.
        if not rows:
            print("No articles found in the database. Run Newsboat to fetch content.")
        else:
            for row in rows:
                article_title, article_url, feed_url = row
                
                # Truncate long strings for better display.
                truncated_title = (article_title[:37] + '...') if len(article_title) > 40 else article_title
                truncated_article_url = (article_url[:37] + '...') if len(article_url) > 40 else article_url
                truncated_feed_url = (feed_url[:37] + '...') if len(feed_url) > 40 else feed_url

                print(f"{truncated_title:<40} | {truncated_article_url:<40} | {truncated_feed_url:<40}")
        
        print("-" * 80)

    except sqlite3.Error as e:
        # Print a helpful error message if there's a problem with the database.
        print(f"SQLite error: {e}")
    finally:
        # Always close the database connection.
        if conn:
            conn.close()

# Run the utility when the script is executed.
if __name__ == "__main__":
    dump_feed_data()

