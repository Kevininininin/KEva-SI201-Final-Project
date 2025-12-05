# API: Weatherstack
# AccuWeather API info gathering -> store it into DB

import requests
import json
import os

API_URL = "http://api.weatherstack.com/current"
API_KEY = "c1cd07cedf6126291b51daf5ce832f8c"

# Request parameters
params = {
    "access_key": API_KEY,
    "query": "New York",   # or "JFK Airport"
    "units": "m"           # metric (default)
}

# Call API
response = requests.get(API_URL, params=params)
data = response.json()

# Check for API-level error
if "error" in data:
    print("API Error:", data["error"].get("info"))
    exit()

# Ensure folder exists
os.makedirs("weather_json_raw_data", exist_ok=True)

# Create timestamp-based filename
timestamp = "2025_Dec_5_Noon"
OUTPUT_FILE = f"weather_json_raw_data/weather_{timestamp}.json"

# Write full JSON response to file
with open(OUTPUT_FILE, "w") as f:
    json.dump(data, f, indent=2)

print(f"Saved weather data to {OUTPUT_FILE}")
