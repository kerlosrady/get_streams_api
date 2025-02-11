import os
import json
import concurrent.futures
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins

# Load Google Credentials from Environment Variable
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

client = None  # Initialize BigQuery client

if credentials_json:
    try:
        credentials_dict = json.loads(credentials_json)
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        client = bigquery.Client(credentials=credentials)
        print("✅ BigQuery authentication successful!")
    except Exception as e:
        print(f"❌ Error loading BigQuery credentials: {e}")
        client = None
else:
    print("❌ No credentials found in environment variable!")

# Dataset list (we will query these in parallel)
dataset_list = [
    "keywords_ranking_data_sheet1",
    "keywords_ranking_data_sheet2",
    "keywords_ranking_data_sheet3",
    "keywords_ranking_data_sheet4"
]

def fetch_data(dataset, table_id):
    """Fetch data from BigQuery for a given dataset & table."""
    try:
        query = f"SELECT * FROM `automatic-spotify-scraper.{dataset}.{table_id}`"
        query_job = client.query(query)
        results = query_job.result()

        data = [dict(row) for row in results]  # Convert to dictionary

        if not data:
            print(f"❌ No data found for {dataset}.{table_id}")
            return None

        print(f"✅ Data retrieved from {dataset}.{table_id}")
        return dataset.replace('keywords_ranking_data_sheet', ''), data

    except Exception as e:
        print(f"❌ Query failed for {dataset}.{table_id}: {e}")
        return None

@app.route('/query_bigquery', methods=['POST'])
def query_bigquery():
    try:
        if client is None:
            return jsonify({"status": "error", "message": "BigQuery client is not initialized. Check credentials."})

        user_input = request.json.get("TABLE_ID")
        if not user_input:
            return jsonify({"status": "error", "message": "TABLE_ID is missing in request."})

        TABLE_ID = user_input
        all_dataframes = {}

        # Use ThreadPoolExecutor for parallel execution
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # Launch parallel tasks for each dataset
            future_to_dataset = {executor.submit(fetch_data, dataset, TABLE_ID): dataset for dataset in dataset_list}

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_dataset):
                result = future.result()
                if result:
                    dataset_name, data = result
                    all_dataframes[dataset_name] = data  # Store results

        return jsonify({"status": "success", "data": all_dataframes})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8080)
