import sqlite3
import json
import os

# --------------------------------------
# Build absolute paths relative to this script
# --------------------------------------

BASE_DIR = os.path.dirname(__file__)

DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "Database", "weather.db"))
WEATHER_FOLDER = os.path.abspath(os.path.join(BASE_DIR, "..", "weather_json_raw_data"))

# List of JSON files to process
weather_files = [
    "weather_2025_Dec_2_Night.json",
    "weather_2025_Dec_3_Afternoon.json",
    "weather_2025_Dec_3_Morning.json",
    "weather_2025_Dec_4_Night.json",
    "weather_2025_Dec_5_Noon.json",
    "weather_2025_Dec_6_Midnight.json",
    "weather_2025_Dec_6_Noon.json"
]

print("DB_PATH =", DB_PATH)
print("WEATHER_FOLDER =", WEATHER_FOLDER)

# --------------------------------------
# Ensure the Database folder exists
# --------------------------------------

db_folder = os.path.dirname(DB_PATH)
if not os.path.exists(db_folder):
    os.makedirs(db_folder)
    print(f"Created folder: {db_folder}")


# --------------------------------------
# Open / create database
# --------------------------------------

try:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
except sqlite3.OperationalError as e:
    print("ERROR opening database:", e)
    exit()


# --------------------------------------
# Create table
# --------------------------------------

cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        wind_speed REAL,
        pressure REAL,
        precip REAL,
        humidity REAL,
        cloudcover REAL,
        feelslike REAL,
        uv_index REAL,
        visibility REAL,
        temperature REAL,
        is_day TEXT,
        localtime TEXT
    )
""")

print("Table 'weather_data' ready.\n")


# --------------------------------------
# Helper to safely extract values
# --------------------------------------

def safe(dictionary, key, default=None):
    return dictionary.get(key, default)


# --------------------------------------
# Loop through files and insert into DB
# --------------------------------------

insert_count = 0

for filename in weather_files:

    full_path = os.path.join(WEATHER_FOLDER, filename)

    if not os.path.exists(full_path):
        print(f"File not found, skipping: {filename}")
        continue

    print(f"Processing: {filename}")

    with open(full_path, "r") as f:
        data = json.load(f)

    current = data.get("current", {})
    location = data.get("location", {})

    # Extract desired fields
    row = (
        safe(current, "wind_speed"),
        safe(current, "pressure"),
        safe(current, "precip"),
        safe(current, "humidity"),
        safe(current, "cloudcover"),
        safe(current, "feelslike"),
        safe(current, "uv_index"),
        safe(current, "visibility"),
        safe(current, "temperature"),
        safe(current, "is_day"),
        safe(location, "localtime"),
    )

    # Insert row
    cur.execute("""
        INSERT INTO weather_data (
            wind_speed, pressure, precip, humidity, cloudcover,
            feelslike, uv_index, visibility, temperature,
            is_day, localtime
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row)

    insert_count += 1


# --------------------------------------
# Save changes and close DB
# --------------------------------------

conn.commit()
conn.close()

print(f"\nDone! Inserted {insert_count} rows into weather_data table.")
print(f"Database saved at: {DB_PATH}")
