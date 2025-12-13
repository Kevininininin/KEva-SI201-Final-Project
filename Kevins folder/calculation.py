# Kevin's graphs
    # Scatter plot comparing avg delay vs. wind speed
    # Scatter plot comparing avg delay vs. humidity 

import sqlite3


# ========== CALCULATION & DATA PREP ==========
def get_avg_delay_by_session():
    """
    Get data from final_project.db
    Return dict {session_name : avg_delay}
    """

    # Connect DB
    conn = sqlite3.connect("Database/final_project.db")
    cur = conn.cursor()

    # Query all needed fields in one go
    query = """
        SELECT weather_sessions.session_name, flights_data.departure_delay
        FROM flights_data
        JOIN weather_sessions
        ON flights_data.weather_id = weather_sessions.id
    """
    cur.execute(query)
    rows = cur.fetchall() #list of tupples [(str session_name, int departure_delay),(),...]

    # Accumulate totals and counts
    totals = {}
    counts = {} 
    debug_delays = {} # Debug: for printing individual delay vals


    for session_name, delay in rows:
        totals[session_name] = totals.get(session_name, 0) + delay
        counts[session_name] = counts.get(session_name, 0) + 1

        if session_name not in debug_delays: # For DEBUG debug_delays
            debug_delays[session_name] = []
        debug_delays[session_name].append(delay)

    # Calculating averages
    return_dict = {}
    for session_name in totals:
        avg_delay = totals[session_name] / counts[session_name]
        return_dict[session_name] = int(round(avg_delay))

    # Close DB
    conn.close()

    # DEBUG: print raw delays for the first session
    # print("\nDEBUG: Raw delays for session:")
    # print(f"Count = {len(debug_delays["2025_Dec_2_Night"])}")
    # for val in debug_delays["2025_Dec_2_Night"]:
    #     print(val)
    # for key, value in debug_delays.items():
    #     print(f"Total count for {key} = {len(value)}")

    # Return result
    return return_dict

def add_weather_to_avg_delay(avg_delay_dict):
    # Connect DB
    conn = sqlite3.connect("Database/final_project.db")
    cur = conn.cursor()

    # Query weather_sessions table
    query = """
        SELECT session_name, wind_speed, humidity
        FROM weather_sessions
    """
    cur.execute(query)
    rows = cur.fetchall() #list of tupples [(str session_name, int wind_speed, int humidity),(),...]

    # Prepare return_dict
    return_dict = {}

    # Enrich avg delay data directly in one loop
    for session_name, wind_speed, humidity in rows:
        if session_name in avg_delay_dict:
            avg_delay = avg_delay_dict[session_name]

            return_dict[session_name] = {
                "avg_delay": avg_delay,
                "wind_speed": wind_speed,
                "humidity": humidity
            }

    # Close DB
    conn.close()

    # Return result
    return return_dict


# ========== WRITING INTO FILE ==========




# ========== PLOTTING ==========
# Scatter plot 1:

# Scatter plot 2:


# ========== MAIN FUNCTION ==========
def main():
    avg_delays = get_avg_delay_by_session()
    avg_delay_enriched = add_weather_to_avg_delay(avg_delays)
    
    # for key, value in avg_delay_enriched.items():
    #     print(f"{key} : {value}")


if __name__ == "__main__":
    main()