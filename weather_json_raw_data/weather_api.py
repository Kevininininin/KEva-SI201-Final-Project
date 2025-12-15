# API: Weatherstack
# Store query locally as a json file titled weather_{timestamp}.json

import requests
import json

API_URL = "http://api.weatherstack.com/current"
API_KEY = "c1cd07cedf6126291b51daf5ce832f8c"

params = {
    "access_key": API_KEY,
    "query": "New York", 
    "units": "m"  # Using metric units
}

response = requests.get(API_URL, params=params)
data = response.json()

if "error" in data:
    print("API Error:", data["error"].get("info"))
    exit()


# ==== Edit File Timestamp Here ====
timestamp = "2025_Dec_6_Noon_DEMO"
OUTPUT_FILE = f"weather_json_raw_data/weather_{timestamp}.json"

with open(OUTPUT_FILE, "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved weather data to {OUTPUT_FILE}")
