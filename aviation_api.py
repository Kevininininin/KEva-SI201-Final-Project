# API: Aviationstack
# Aviation API info gathering -> store it into DB

import requests
import json
import os

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

# Ensure folder exists
os.makedirs("aviation_json_raw_data", exist_ok=True)

timestamp = "2025_Dec_2_Night"
OUTPUT_FILE = f"aviation_json_raw_data/flights_{timestamp}.json"

with open(OUTPUT_FILE, "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved {len(data.get('data', []))} flights to {OUTPUT_FILE}")
