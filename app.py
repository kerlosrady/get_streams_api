import os
import json
import pandas as pd
import concurrent.futures  # Multi-threading
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account

# ------------------------
# Initialize Flask app and enable CORS
# ------------------------
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# ------------------------
# BigQuery Authentication
# ------------------------
PROJECT_ID = "automatic-spotify-scraper"

# Get credentials from environment or use local file
creds_input = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "automatic-spotify-scraper.json")

try:
    if creds_input.strip().endswith(".json"):
        credentials = service_account.Credentials.from_service_account_file(creds_input)
    else:
        creds_info = json.loads(creds_input)
        credentials = service_account.Credentials.from_service_account_info(creds_info)

    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print("✅ BigQuery authentication successful!")
except Exception as e:
    print(f"❌ Failed to authenticate with BigQuery: {e}")
    client = None


# ------------------------
# Helper function: Fetch BigQuery Data (Optimized)
# ------------------------
def fetch_data(dataset, table, playlist_id):
    """
    Fetch relevant columns from a BigQuery table, filtering by playlist_id.
    """
    query = f"""
        SELECT 
            `Spotify Playlist URL`, `Track Count`, `Estimate Total`, 
            `1st`, `2 - 10`, `11 - 20`, `21 - 50`, `+50`,
            `1 estimate`, `2 - 10 estimate`, `11 - 20 estimate`, `21 - 50 estimate`, `+50 estimate`
        FROM `{PROJECT_ID}.{dataset}.{table}`
        WHERE `Spotify Playlist URL` LIKE '%{playlist_id}%'
        LIMIT 1
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
        data = [dict(row) for row in results]
        return data[0] if data else None
    except Exception as e:
        print(f"⚠️ Query Error for {dataset}.{table}: {e}")
        return None


# ------------------------
# Main Function: get_playlist_ids (Optimized)
# ------------------------
def get_playlist_ids(playlist_url):
    """
    Fetches track count and estimates from BigQuery efficiently.
    Uses multi-threading to fetch data in parallel.
    """
    playlist_id = playlist_url.split('?')[0].replace('https://open.spotify.com/playlist/', '')

    dataset = "global_stream_tracker"
    tables = ["jan_data", "dec_data", "nov_data", "oct_data", "sep_data"]
    
    # Use ThreadPoolExecutor to run queries in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_table = {
            executor.submit(fetch_data, dataset, table, playlist_id): table for table in tables
        }

        # Collect results
        results = {}
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                data = future.result()
                results[table_name] = data if data else {"error": "No data found"}
            except Exception as e:
                results[table_name] = {"error": str(e)}

    # Extract values
    track_count = results.get("jan_data", {}).get("Track Count", "?")
    est0 = results.get("jan_data", {}).get("Estimate Total", "?")
    est1 = results.get("dec_data", {}).get("Estimate Total", "?")
    est2 = results.get("nov_data", {}).get("Estimate Total", "?")
    est3 = results.get("oct_data", {}).get("Estimate Total", "?")
    est4 = results.get("sep_data", {}).get("Estimate Total", "?")
    
    lssst = results.get("jan_data", {})
    lssst = [lssst.get(col, "?") for col in [
        "1st", "2 - 10", "11 - 20", "21 - 50", "+50",
        "1 estimate", "2 - 10 estimate", "11 - 20 estimate", "21 - 50 estimate", "+50 estimate"
    ]]

    return {
        "track_count": track_count,
        "estimates": [est0, est1, est2, est3, est4],
        "lssst": lssst
    }


# ------------------------
# API Endpoint: /get_playlist_ids
# ------------------------
@app.route('/get_playlist_ids', methods=['POST'])
def api_get_playlist_ids():
    try:
        data = request.get_json()
        if not data or "playlist_url" not in data:
            return jsonify({"status": "error", "message": "Missing 'playlist_url' in request."}), 400
        
        playlist_url = data["playlist_url"]
        results = get_playlist_ids(playlist_url)
        return jsonify({"status": "success", "data": results})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ------------------------
# Main entry point for local testing
# ------------------------
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8080)
