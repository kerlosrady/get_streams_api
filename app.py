import os
import json
import pandas as pd
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

# Get the environment variable; if not set, default to the filename.
creds_input = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "automatic-spotify-scraper.json")

try:
    # If the environment variable ends with ".json", assume it's a file path.
    if creds_input.strip().endswith(".json"):
        credentials = service_account.Credentials.from_service_account_file(creds_input)
    else:
        # Otherwise, assume it's a JSON string.
        creds_info = json.loads(creds_input)
        credentials = service_account.Credentials.from_service_account_info(creds_info)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print("✅ BigQuery authentication successful!")
except Exception as e:
    print(f"❌ Failed to authenticate with BigQuery: {e}")
    client = None

# ------------------------
# Helper function: read_from_bigquery
# ------------------------
def read_from_bigquery(dataset, table):
    """
    Read data from a BigQuery table and return as a Pandas DataFrame.
    """
    query = f"SELECT * FROM `{PROJECT_ID}.{dataset}.{table}` LIMIT 10000"
    query_job = client.query(query)
    results = query_job.result()
    data = [dict(row) for row in results]
    return pd.DataFrame(data)

# ------------------------
# Helper function: clean playlist id
# ------------------------
def clean_playlist_id(playlist_url):
    """Extract the playlist id from a Spotify playlist URL."""
    return playlist_url.split('?')[0].replace('https://open.spotify.com/playlist/','')

# ------------------------
# Main function: get_playlist_ids
# ------------------------
def get_playlist_ids(playlist_url):
    """
    Given a Spotify playlist URL, query five BigQuery tables (jan_data, dec_data, nov_data, oct_data, sep_data)
    from the 'global_stream_tracker' dataset, and return a collection of estimates and track counts.
    """
    # Read all required tables from BigQuery
    df_jan = read_from_bigquery('global_stream_tracker', 'jan_data')
    df_dec = read_from_bigquery('global_stream_tracker', 'dec_data')
    df_nov = read_from_bigquery('global_stream_tracker', 'nov_data')
    df_oct = read_from_bigquery('global_stream_tracker', 'oct_data')
    df_sep = read_from_bigquery('global_stream_tracker', 'sep_data')
    
    # Clean the playlist id from the URL
    playlist_id = clean_playlist_id(playlist_url)
    
    # Helper: Get a single value from a column where 'Spotify Playlist URL' contains the playlist id.
    def get_value(df, column, default='?'):
        values = df.loc[df['Spotify Playlist URL'].str.contains(playlist_id, na=False), column].values
        return values[0] if len(values) > 0 else default

    # Get Track Count from jan_data
    track_count = get_value(df_jan, 'Track Count', default='?')
    
    # Get Estimate Total values from each table
    est0 = get_value(df_jan, 'Estimate Total', default='?')
    est1 = get_value(df_dec, 'Estimate Total', default='?')
    est2 = get_value(df_nov, 'Estimate Total', default='?')
    est3 = get_value(df_oct, 'Estimate Total', default='?')
    est4 = get_value(df_sep, 'Estimate Total', default='?')
    
    # Get additional estimates from jan_data (10 columns)
    estimate_columns = ['1st', '2 - 10', '11 - 20', '21 - 50', '+50',
                        '1 estimate', '2 - 10 estimate', '11 - 20 estimate', '21 - 50 estimate', '+50 estimate']
    lssst_values = df_jan.loc[df_jan['Spotify Playlist URL'].str.contains(playlist_id, na=False), estimate_columns].values
    lssst = list(lssst_values[0]) if len(lssst_values) > 0 else ['?'] * len(estimate_columns)
    
    # Return all values in a dictionary
    return {
        "est0": est0,
        "est1": est1,
        "est2": est2,
        "est3": est3,
        "est4": est4,
        "lssst": lssst,
        "track_count": track_count
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
