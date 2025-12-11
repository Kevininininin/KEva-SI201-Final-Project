import sqlite3
import os

# ==========================================
# 1. SETUP PATHS
# ==========================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FOLDER = os.path.join(BASE_DIR, "..", "Database")
DB_PATH = os.path.join(DB_FOLDER, "final_project.db")

print(f"Connecting to database at: {DB_PATH}")

# ==========================================
# 2. PERFORM THE SIMPLIFIED JOIN
# ==========================================

try:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Step A: Delete the table if it exists so we can recreate it fresh
    cur.execute("DROP TABLE IF EXISTS flight_weather_master")
    print("Old table dropped (if it existed).")

    # Step B: Create the new table using a JOIN query
    # REVISION: Only joining flights_data (f) and weather_data (w)
    create_query = """
    CREATE TABLE flight_weather_master AS
    SELECT 
        f.flight_id,
        f.session_id,
        f.flight_number,
        f.airline_name,
        f.departure_delay,
        f.flight_status,
        f.departure_airport,
        f.scheduled_time,
        
        w.wind_speed,
        w.pressure,
        w.precip,
        w.humidity,
        w.cloudcover,
        w.feelslike,
        w.uv_index,
        w.visibility,
        w.temperature,
        w.is_day,
        w.localtime
        
    FROM flights_data f
    JOIN weather_data w ON f.session_id = w.session_id
    """

    cur.execute(create_query)
    print("New table 'flight_weather_master' created successfully.")

    # Step C: Verification - Print the first 5 rows
    print("\n--- Preview of Joined Data (First 5 Rows) ---")
    cur.execute("SELECT * FROM flight_weather_master LIMIT 5")
    rows = cur.fetchall()
    
    # Get column names for display
    col_names = [description[0] for description in cur.description]
    print(f"Columns: {col_names}")
    
    for row in rows:
        print(row)

    # Step D: Check total count
    cur.execute("SELECT COUNT(*) FROM flight_weather_master")
    count = cur.fetchone()[0]
    print(f"\nTotal rows in master table: {count}")

    conn.commit()
    conn.close()

except sqlite3.OperationalError as e:
    print(f"Error: {e}")
    print("Make sure 'final_project.db' exists and has the source tables created.")