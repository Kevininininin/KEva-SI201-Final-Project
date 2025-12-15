# API: Aviationstack
# Store query locally as a json file titled aviation_{timestamp}.json

import requests
import json

API_KEY = "7d02e037d5679bb3416ea07be47ede3b"

params = {
    "access_key": API_KEY,
    "dep_iata": "JFK",
    "limit": 100,
    "offset": 0
}

response = requests.get("http://api.aviationstack.com/v1/flights", params=params)
data = response.json()

if "error" in data:
    print("API Error:", data["error"])
    exit()


# ==== Edit File Timestamp Here ====
timestamp = "2025_Dec_6_Noon_DEMO"
OUTPUT_FILE = f"aviation_json_raw_data/flights_{timestamp}.json"


with open(OUTPUT_FILE, "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved {len(data.get('data', []))} flights to {OUTPUT_FILE}")
