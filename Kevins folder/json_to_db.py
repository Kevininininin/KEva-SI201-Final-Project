import sqlite3
import json
import os

# ==== SETTING UP ====

# Defining file directories & paths
BASE_DIR = os.path.dirname(__file__)
AVIATION_FOLDER = os.path.join(BASE_DIR, "..", "aviation_json_raw_data")
WEATHER_FOLDER = os.path.join(BASE_DIR, "..", "weather_json_raw_data")
DB_FOLDER = os.path.join(BASE_DIR, "..", "Database")
DB_PATH = os.path.join(DB_FOLDER, "final_project.db")


# Process sequence, determined by Kevin
session_timestamps = [
    "2025_Dec_2_Night",
    "2025_Dec_3_Afternoon",
    "2025_Dec_3_Morning",
    "2025_Dec_4_Night",
    "2025_Dec_5_Noon",
    "2025_Dec_6_Midnight",
    "2025_Dec_6_Noon"
]

# print(f"Database will be saved to: {DB_PATH}")



# ==== CREATING DATABASE ====

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Enable Foreign Key support
cur.execute("PRAGMA foreign_keys = ON;")

# A. Create the 'Hub' Table (capture_sessions)
cur.execute("""
    CREATE TABLE IF NOT EXISTS capture_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_name TEXT UNIQUE
    )
""")

# B. Create the Weather Table (Linked to session)
cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER,
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
        localtime TEXT,
        FOREIGN KEY (session_id) REFERENCES capture_sessions (id)
    )
""")

# C. Create the Flights Table (Linked to session)
cur.execute("""
    CREATE TABLE IF NOT EXISTS flights_data (
        flight_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER,
        flight_number TEXT,
        airline_name TEXT,
        departure_delay INTEGER,
        flight_status TEXT,
        departure_airport TEXT,
        scheduled_time TEXT,
        FOREIGN KEY (session_id) REFERENCES capture_sessions (id)
    )
""")

# Clear existing data to prevent duplicates if you run this script multiple times
# (Since we are re-processing files from scratch)
cur.execute("DELETE FROM flights_data")
cur.execute("DELETE FROM weather_data")
cur.execute("DELETE FROM capture_sessions")
conn.commit()
print("Tables initialized and cleared.\n")



# ==== HELPER FUNCTIONS ====

def get_json_data(folder, filename):
    """Safely loads JSON data from a file."""
    path = os.path.join(folder, filename)
    if not os.path.exists(path):
        print(f"Warning: File not found: {path}")
        return None
    try:
        with open(path, "r", encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return None

def safe_get(d, key, default=None):
    """Helper to get value from dict safely."""
    if not isinstance(d, dict):
        return default
    return d.get(key, default)



# ==== MAIN LOOP ====

total_flights_inserted = 0
total_weather_inserted = 0

for timestamp in session_timestamps:
    print(f"--- Processing Session: {timestamp} ---")

    # ----------------------------------------
    # STEP 1: Register the Session (The Hub)
    # ----------------------------------------
    cur.execute("INSERT INTO capture_sessions (session_name) VALUES (?)", (timestamp,))
    # Retrieve the auto-generated ID for this session
    current_session_id = cur.lastrowid


    # ----------------------------------------
    # STEP 2: Process Weather
    # ----------------------------------------
    weather_filename = f"weather_{timestamp}.json"
    w_data = get_json_data(WEATHER_FOLDER, weather_filename)

    if w_data:
        curr = w_data.get("current", {})
        loc = w_data.get("location", {})

        cur.execute("""
            INSERT INTO weather_data (
                session_id, wind_speed, pressure, precip, humidity, cloudcover,
                feelslike, uv_index, visibility, temperature, is_day, localtime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            current_session_id,
            safe_get(curr, "wind_speed"),
            safe_get(curr, "pressure"),
            safe_get(curr, "precip"),
            safe_get(curr, "humidity"),
            safe_get(curr, "cloudcover"),
            safe_get(curr, "feelslike"),
            safe_get(curr, "uv_index"),
            safe_get(curr, "visibility"),
            safe_get(curr, "temperature"),
            safe_get(curr, "is_day"),
            safe_get(loc, "localtime")
        ))
        total_weather_inserted += 1


    # ----------------------------------------
    # STEP 3: Process Flights
    # ----------------------------------------
    flight_filename = f"flights_{timestamp}.json"
    f_data = get_json_data(AVIATION_FOLDER, flight_filename)

    if f_data:
        # Get the list of flights, default to empty list if key missing
        flights_list = f_data.get("data", [])
        
        # SLICE: Take only the first 50 flights as requested
        flights_subset = flights_list[:50]

        for flight in flights_subset:
            # Extract nested dictionaries
            dep = flight.get("departure", {}) or {}
            airline = flight.get("airline", {}) or {}
            flight_info = flight.get("flight", {}) or {}

            # Handle Delay: If None/Null, replace with 0
            raw_delay = safe_get(dep, "delay")
            if raw_delay is None:
                cleaned_delay = 0
            else:
                cleaned_delay = int(raw_delay)

            cur.execute("""
                INSERT INTO flights_data (
                    session_id, flight_number, airline_name, 
                    departure_delay, flight_status, departure_airport, scheduled_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                current_session_id,
                safe_get(flight_info, "iata"),       # Flight Number (e.g. AV7393)
                safe_get(airline, "name"),           # Airline Name
                cleaned_delay,                       # CLEANED Delay (0 instead of None)
                safe_get(flight, "flight_status"),   # Status
                safe_get(dep, "iata"),               # Airport Code (e.g. JFK)
                safe_get(dep, "scheduled")           # Scheduled Time
            ))
            total_flights_inserted += 1


conn.commit()
conn.close()

print("\n" + "="*30)
print("SUCCESS!")
print(f"Total Sessions Created: {len(session_timestamps)}")
print(f"Total Weather Rows:     {total_weather_inserted}")
print(f"Total Flight Rows:      {total_flights_inserted}")
print("="*30)