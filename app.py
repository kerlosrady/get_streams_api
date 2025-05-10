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
PROJECT_ID = "desktop-spotify-400111"

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
            `Playlist`, `Spotify Playlist URL`, `Followers`, `Track Count`, `total_stream_estimate`, `Curator`,
            `1_isitagoodplaylist`, `2-10_isitagoodplaylist`, `11-20_isitagoodplaylist`, `21-50_isitagoodplaylist`, `50+_isitagoodplaylist`,
            `estimated_1st`, `estimated_2_10`, `estimated_11_20`, `estimated_21_50`, `estimated_+50`, `streams_april`, `streams_march`,
            `streams_feb`, `streams_jan`
        FROM `{PROJECT_ID}.{dataset}.{table}`
        WHERE `Spotify Playlist URL` LIKE '%{playlist_id}%'
        LIMIT 1
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
        print(results)
        data = [dict(row) for row in results]
        return data[0] if data else None
    except Exception as e:
        print(f"⚠️ Query Error for {dataset}.{table}: {e}")
        return None

def fetch_data2(dataset, table, playlist_id):
    """
    Fetch relevant columns from a BigQuery table, filtering by playlist_id.
    """
    query = f"""
        SELECT
            `ad link`
        FROM `{PROJECT_ID}.{dataset}.{table}`
        WHERE `playlist url` LIKE '%{playlist_id}%'
        LIMIT 5
    """

    try:
        query_job = client.query(query)
        results = query_job.result()
        results={'ad link' : [x['ad link'] for x in results]}
        return results if len(results['ad link'])>0 else []
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
    # tables = ['april_data', "march_data", "jan_data", "dec_data", "nov_data", "oct_data", "sep_data"]
    tables = ['merged_may_all']

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

    dataset = "ads_data"
    # tables = ['april_data', "march_data", "jan_data", "dec_data", "nov_data", "oct_data", "sep_data"]
    tables = ['ads_data']

    # Use ThreadPoolExecutor to run queries in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_table = {
            executor.submit(fetch_data2, dataset, table, playlist_id): table for table in tables
        }

        # Collect results
        results2 = {}
        for future in concurrent.futures.as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                data = future.result()
                results2[table_name] = data if data else {"error": "No data found"}
            except Exception as e:
                results2[table_name] = {"error": str(e)}
    # Extract values
    track_count = results.get("merged_may_all", {}).get("Track Count", "?")
    est0 = results.get("merged_may_all", {}).get("total_stream_estimate", "?")
    followers = results.get("merged_may_all", {}).get("Followers", "?")
    curator = results.get("merged_may_all", {}).get("Curator", "?")
    playlist_name = results.get("merged_may_all", {}).get("Playlist", "?")

    est1 = results.get("merged_may_all", {}).get("streams_april", "?")
    est2 = results.get("merged_may_all", {}).get("streams_march", "?")
    est3 = results.get("merged_may_all", {}).get("streams_feb", "?")
    est4 = results.get("merged_may_all", {}).get("streams_jan", "?")
    # est4 = results.get("oct_data", {}).get("Estimate Total", "?")
    # est4 = results.get("sep_data", {}).get("Estimate Total", "?")
    ads_links= results2.get("ads_data", {}).get("ad link", [])
    for x in range(0,5-len(ads_links)):
        ads_links.append("")
    print(ads_links)
    lssst = results.get("merged_may_all", {})
    lssst = [lssst.get(col, "?") for col in ["1_isitagoodplaylist", "2-10_isitagoodplaylist", "11-20_isitagoodplaylist",
                                             "21-50_isitagoodplaylist", "50+_isitagoodplaylist", "estimated_1st", "estimated_2_10", "estimated_11_20", "estimated_21_50", "estimated_+50" ]]

    return {
        "track_count": track_count,
        "estimates": [est0, est1, est2, est3, est4],
        "lssst": lssst,
        'Followers': followers,
        'curator': curator,
        'playlist_name': playlist_name,
        'ads_links': ads_links
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
