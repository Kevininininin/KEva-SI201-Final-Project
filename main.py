# Name: Jinghan Wu, Eva Park
# UMID: 77132944, 07553115
# E-mail: kevinwuu@umich.edu, evapark@umich.edu
# AI disclosure: TBD

import sqlite3
import json
import requests
import time

def debug_print_flights(session_name, start_flight_num, end_flight_num):
    file_path = f"aviation_json_raw_data/flights_{session_name}.json"

    with open(file_path, "r") as f:
        flight_json = json.load(f)

    all_flights = flight_json.get("data", [])
    total_flights = len(all_flights)
    print(f"Loaded {total_flights} flights from {file_path}\n")

    start_index = start_flight_num - 1

    end_index = min(end_flight_num, total_flights)


    print(f"Printing Flight {start_index + 1} through Flight {end_index}\n")

    for i in range(start_index, end_index):
        flight = all_flights[i]

        airline_info = flight.get("airline", {})
        airline_name = airline_info.get("name", "Unknown")

        flight_info = flight.get("flight", {})
        flight_iata = flight_info.get("iata", "Unknown")

        departure_info = flight.get("departure", {})
        arrival_info = flight.get("arrival", {})

        departure_iata = departure_info.get("iata", "Unknown")
        arrival_iata = arrival_info.get("iata", "Unknown")

        scheduled_time = departure_info.get("scheduled", "Unknown")

        delay = departure_info.get("delay")
        delay_value = delay if delay is not None else 0

        print(f"Flight {i + 1}")
        print(f"    Airline: {airline_name}")
        print(f"    Flight IATA: {flight_iata}")
        print(f"    Destination: {departure_iata} -> {arrival_iata}")
        print(f"    Scheduled: {scheduled_time}")
        print(f"    Delay: {delay_value}")
        print()

# ========
def create_database():
    conn = sqlite3.connect("Database/final_project.db")
    cur = conn.cursor()

    # Enable foreign key constraints
    cur.execute("PRAGMA foreign_keys = ON;")

    # Create weather_sessions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_sessions (
            id INTEGER PRIMARY KEY,
            wind_speed REAL,
            humidity REAL,
            is_day INTEGER
        )
    """)

    # Create airlines table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS airlines (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE
        )
    """)

    # Create flights_data table
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

    conn.commit()
    conn.close()

    """
    Purpose:
    - Insert flight records linked to a weather session
    - Normalize airline names
    """

    # connect to database

    # for each [airline_name, delay] in flight_list:

        # Track time inserted. if 25 entries has already been inserted, then break.

        # insert airline if not exists
            # INSERT OR IGNORE INTO airlines(name)

        # select airline_id from airlines

        # insert into flights_data:
            # weather_id
            # airline_id
            # departure_delay

    # commit and close

def fetch_weather_data():
    """
    Call Weatherstack API and return weather values.
    """

    API_URL = "http://api.weatherstack.com/current"
    API_KEY = "c1cd07cedf6126291b51daf5ce832f8c"

    params = {
        "access_key": API_KEY,
        "query": "New York",
        "units": "m" # set unit to metrics just cuz both Kevin & Eva use metrics
    }

    response = requests.get(API_URL, params=params)
    data = response.json()

    if "error" in data:
        print("Weather API Error:", data["error"])
        return None

    # Picking out data of interests
    current = data.get("current", {})

    wind_speed = current.get("wind_speed", 0.0)
    humidity = current.get("humidity", 0.0)

    is_day_raw = str(current.get("is_day", "no")).lower()
    is_day = 1 if (is_day_raw == "yes") else 0

    return {
        "wind_speed": wind_speed,
        "humidity": humidity,
        "is_day": is_day
    }

def insert_weather_session(weather_data):
    """
    Takes in a dict of 
    {
        "wind_speed": __,
        "humidity": __,
        "is_day": __
    }
    """

    conn = sqlite3.connect("Database/final_project.db")
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO weather_sessions (wind_speed, humidity, is_day)
        VALUES (?, ?, ?)
    """, (
        weather_data["wind_speed"],
        weather_data["humidity"],
        weather_data["is_day"]
    ))

    weather_id = cur.lastrowid

    conn.commit()
    conn.close()

    return weather_id

def fetch_flight_data(offset):
    """
    Call Aviationstack API, over-fetch 50, return up to 25 valid flights.
    """

    API_URL = "http://api.aviationstack.com/v1/flights"
    API_KEY = "7d02e037d5679bb3416ea07be47ede3b"

    params = {
        "access_key": API_KEY,
        "dep_iata": "JFK",
        "limit": 50,
        "offset": offset
    }

    response = requests.get(API_URL, params=params)
    data = response.json()

    if "error" in data:
        print("Aviation API Error:", data["error"])
        return []

    valid_flights = [] # For return

    for flight in data.get("data", []):

        airline_info = flight.get("airline", {})
        airline_name = airline_info.get("name")

        if not airline_name:
            continue

        departure_info = flight.get("departure", {})
        delay = departure_info.get("delay")
        delay_value = int(delay) if delay is not None else 0

        valid_flights.append([airline_name, delay_value])

        if len(valid_flights) == 25:
            break

    return valid_flights

def insert_flight_records(flight_list, weather_id):
    conn = sqlite3.connect("Database/final_project.db")
    cur = conn.cursor()

    inserted_count = 0

    for airline_name, delay in flight_list:

        if inserted_count >= 25:
            break

        # Insert airline if it doesn't exist
        cur.execute("""
            INSERT OR IGNORE INTO airlines (name)
            VALUES (?)
        """, (airline_name,))

        # Get airline_id
        cur.execute("""
            SELECT id FROM airlines WHERE name = ?
        """, (airline_name,))
        airline_id = cur.fetchone()[0]

        # Insert flight record
        cur.execute("""
            INSERT INTO flights_data (weather_id, airline_id, departure_delay)
            VALUES (?, ?, ?)
        """, (weather_id, airline_id, delay))

        inserted_count += 1

    conn.commit()
    conn.close()



def main():
    create_database()

    # Configuration
    num_sessions = 5
    flight_offset_step = 50
    
    current_offset = 0

    print("Starting Program...")
    print("-----------------------------------")

    # STEP 3: main loop
    for session_index in range(num_sessions):

        print(f"--- Processing session {session_index + 1} of {num_sessions} ---")

        # WEATHER API call & insert to .db
        weather_data = fetch_weather_data()
        if weather_data is None:
            print("Weather data fetch failed.\n PROGRAM TERMINATED")
            break
        else:
            print("Weather data fetched.")

        weather_id = insert_weather_session(weather_data)
        print(f"Weather session inserted with ID: {weather_id}")

        # FLIGHTS API call & insert to .db
        flight_list = fetch_flight_data(current_offset)
        print(f"Fetched {len(flight_list)} valid flight records, API call offset = {current_offset}")

        insert_flight_records(flight_list, weather_id)
        print("Flight records inserted successfully.")

        # OFFSET UPDATE
        current_offset += flight_offset_step

        print()
        time.sleep(5)
        

    # STEP 4: done
    print("-----------------------------------")
    print("Program Complete")



# ========
# For debugging locally stored json files

# def main():
#     # session_name = "2025_Dec_2_Night"
#     # start = 0
#     # end = 10
#     # debug_print_flights(session_name, start, end)
#     pass

if __name__ == "__main__":
    main()