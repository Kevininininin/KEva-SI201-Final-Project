import sqlite3
import json

# ======== SETTING UP ========

# Connect to SQLite database
conn = sqlite3.connect("Database/final_project_2.db")
cur = conn.cursor()

# Enable foreign key constraints
cur.execute("PRAGMA foreign_keys = ON;")


# Timestamp session for file reading sequence
session_timestamps = [
    "2025_Dec_2_Night",
    "2025_Dec_3_Afternoon",
    "2025_Dec_3_Morning",
    "2025_Dec_4_Night",
    "2025_Dec_5_Noon",
    "2025_Dec_6_Midnight",
    "2025_Dec_6_Noon"
]


# ======== CREATE TABLES ========

# Table 1: weather_sessions
cur.execute("""
CREATE TABLE IF NOT EXISTS weather_sessions (
    id INTEGER PRIMARY KEY,
    session_name TEXT,
    wind_speed REAL,
    humidity REAL,
    is_day BOOLEAN
)
""")

# Table 2: airlines
cur.execute("""
CREATE TABLE IF NOT EXISTS airlines (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
)
""")

# Table 3: flights_data
cur.execute("""
CREATE TABLE IF NOT EXISTS flights_data (
    id INTEGER PRIMARY KEY,
    weather_id INTEGER,
    airline_id INTEGER,
    departure_delay INTEGER,
    FOREIGN KEY (weather_id) REFERENCES weather_sessions(id),
    FOREIGN KEY (airline_id) REFERENCES airlines(id)
)
""")


# Clear existing data for rerun
cur.execute("DELETE FROM flights_data")
cur.execute("DELETE FROM airlines")
cur.execute("DELETE FROM weather_sessions")


# ======== MAIN LOOP ========
for session in session_timestamps:

    # ======== LOAD WEATHER JSON ========
    weather_path = f"weather_json_raw_data/weather_{session}.json"

    with open(weather_path, "r") as wf:
        weather_json = json.load(wf)

    current = weather_json.get("current", {})

    wind_speed = current.get("wind_speed", 0.0)
    humidity = current.get("humidity", 0.0)

    # Convert is_day to boolean integer
    is_day_raw = current.get("is_day", "no")
    is_day_str = str(is_day_raw).lower() #normalize it just in case

    if is_day_str == "yes":
        is_day_value = 1
    else:
        is_day_value = 0


    # Insert weather session
    cur.execute("""
        INSERT INTO weather_sessions
        (session_name, wind_speed, humidity, is_day)
        VALUES (?, ?, ?, ?)
    """, (session, wind_speed, humidity, is_day_value))

    weather_id = cur.lastrowid # Prep to insert as forign key in flight_data table 


    # ======== LOAD FLIGHT JSON ========

    flight_path = f"aviation_json_raw_data/flights_{session}.json"

    with open(flight_path, "r") as ff:
        flight_json = json.load(ff)

    # Get all flights from JSON
    all_flights = flight_json.get("data", [])

    # Process flights in batches of 25
    for i in range(0, len(all_flights), 25): # i will be 0, 25, 50, 75, ...

        batch = all_flights[i : i + 25]

        # ======== PROCESS EACH FLIGHT IN THIS BATCH ========
        for flight in batch:

            airline_info = flight.get("airline", {})
            airline_name = airline_info.get("name")

            if not airline_name:
                continue

            # Insert airline if new
            cur.execute("""
                INSERT OR IGNORE INTO airlines (name)
                VALUES (?)
            """, (airline_name,))

            # Get airline_id
            cur.execute("""
                SELECT id FROM airlines WHERE name = ?
            """, (airline_name,))
            airline_id = cur.fetchone()[0] # Prep to insert as forign key in flight_data table 

            # Extract delay (default to 0)
            departure = flight.get("departure", {})
            delay = departure.get("delay")
            if delay is not None:
                departure_delay = int(delay)
            else:
                departure_delay = 0

            # Insert flight record
            cur.execute("""
                INSERT INTO flights_data
                (weather_id, airline_id, departure_delay)
                VALUES (?, ?, ?)
            """, (weather_id, airline_id, departure_delay))

conn.commit()
conn.close()
