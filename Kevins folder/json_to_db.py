import sqlite3
import json
import os

# ==========================================
# 1. SETUP PATHS
# ==========================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AVIATION_FOLDER = os.path.join(BASE_DIR, "..", "aviation_json_raw_data")
WEATHER_FOLDER = os.path.join(BASE_DIR, "..", "weather_json_raw_data")
DB_FOLDER = os.path.join(BASE_DIR, "..", "Database")
DB_PATH = os.path.join(DB_FOLDER, "final_project.db")

if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)

session_timestamps = [
    "2025_Dec_2_Night", "2025_Dec_3_Afternoon", "2025_Dec_3_Morning",
    "2025_Dec_4_Night", "2025_Dec_5_Noon", "2025_Dec_6_Midnight",
    "2025_Dec_6_Noon"
]

print(f"Database will be saved to: {DB_PATH}")

# ==========================================
# 2. DATABASE INITIALIZATION
# ==========================================

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("PRAGMA foreign_keys = ON;")

# A. Merged Weather + Session Table (Solves "Split Table" Risk)
cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_name TEXT UNIQUE,
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

# B. Airlines Table (Solves "Duplicate String" & "Shared Key" Rules)
cur.execute("""
    CREATE TABLE IF NOT EXISTS airlines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        iata TEXT
    )
""")

# C. Flights Table (Links to both Weather and Airlines)
cur.execute("""
    CREATE TABLE IF NOT EXISTS flights_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        weather_id INTEGER,
        airline_id INTEGER,
        flight_number TEXT,
        departure_delay INTEGER,
        flight_status TEXT,
        departure_airport TEXT,
        scheduled_time TEXT,
        FOREIGN KEY (weather_id) REFERENCES weather_sessions (id),
        FOREIGN KEY (airline_id) REFERENCES airlines (id)
    )
""")

# Clear old data to start fresh
cur.execute("DELETE FROM flights_data")
cur.execute("DELETE FROM airlines")
cur.execute("DELETE FROM weather_sessions")
conn.commit()
print("Tables initialized and cleared.\n")

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def get_json(folder, filename):
    path = os.path.join(folder, filename)
    if not os.path.exists(path):
        print(f"Missing: {path}")
        return None
    try:
        with open(path, "r", encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error {filename}: {e}")
        return None

def safe_get(d, key, default=None):
    if not isinstance(d, dict): return default
    return d.get(key, default)

# ==========================================
# 4. EXECUTION LOOP
# ==========================================

total_flights = 0
# Cache to avoid querying DB for airline ID every single time
# Format: {"Airline Name": airline_db_id}
airline_cache = {} 

for timestamp in session_timestamps:
    print(f"Processing: {timestamp}")

    # --- STEP 1: Insert Weather Session ---
    w_data = get_json(WEATHER_FOLDER, f"weather_{timestamp}.json")
    
    # Defaults
    wind = press = precip = hum = cloud = feel = uv = vis = temp = 0
    day = time = "N/A"

    if w_data:
        curr = w_data.get("current", {})
        loc = w_data.get("location", {})
        wind = safe_get(curr, "wind_speed")
        press = safe_get(curr, "pressure")
        precip = safe_get(curr, "precip")
        hum = safe_get(curr, "humidity")
        cloud = safe_get(curr, "cloudcover")
        feel = safe_get(curr, "feelslike")
        uv = safe_get(curr, "uv_index")
        vis = safe_get(curr, "visibility")
        temp = safe_get(curr, "temperature")
        day = safe_get(curr, "is_day")
        time = safe_get(loc, "localtime")

    cur.execute("""
        INSERT INTO weather_sessions 
        (session_name, wind_speed, pressure, precip, humidity, cloudcover, 
         feelslike, uv_index, visibility, temperature, is_day, localtime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (timestamp, wind, press, precip, hum, cloud, feel, uv, vis, temp, day, time))
    
    current_weather_id = cur.lastrowid

    # --- STEP 2: Process Flights & Airlines ---
    f_data = get_json(AVIATION_FOLDER, f"flights_{timestamp}.json")
    if f_data:
        flights_list = f_data.get("data", [])[:50] # Limit to 50

        for flight in flights_list:
            # Extract Info
            airline_obj = flight.get("airline", {})
            flight_obj = flight.get("flight", {})
            dep_obj = flight.get("departure", {})
            
            air_name = safe_get(airline_obj, "name", "Unknown Airline")
            air_iata = safe_get(airline_obj, "iata", "XX")

            # --- HANDLE AIRLINE (Normalization) ---
            # Check if we've seen this airline in this run
            if air_name not in airline_cache:
                # Check DB just in case (e.g. from previous loop iteration not in cache?)
                # Actually, cache persists across loops, so just insert if new.
                cur.execute("INSERT OR IGNORE INTO airlines (name, iata) VALUES (?, ?)", 
                           (air_name, air_iata))
                # Get the ID
                cur.execute("SELECT id FROM airlines WHERE name = ?", (air_name,))
                airline_id = cur.fetchone()[0]
                airline_cache[air_name] = airline_id
            else:
                airline_id = airline_cache[air_name]

            # --- INSERT FLIGHT ---
            # Clean delay
            raw_delay = safe_get(dep_obj, "delay")
            cleaned_delay = int(raw_delay) if raw_delay is not None else 0

            cur.execute("""
                INSERT INTO flights_data (
                    weather_id, airline_id, flight_number, 
                    departure_delay, flight_status, departure_airport, scheduled_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                current_weather_id,
                airline_id,
                safe_get(flight_obj, "iata"),
                cleaned_delay,
                safe_get(flight, "flight_status"),
                safe_get(dep_obj, "iata"),
                safe_get(dep_obj, "scheduled")
            ))
            total_flights += 1

conn.commit()
conn.close()

print("="*30)
print(f"Done! {total_flights} flights inserted.")
print(f"Unique Airlines Found: {len(airline_cache)}")
print("="*30)