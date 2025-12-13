# Kevin's graphs
    # Scatter plot comparing avg delay vs. wind speed
    # Scatter plot comparing avg delay vs. humidity 

import sqlite3


# ========== CALCULATION & DATA PREP ==========
def get_avg_delay_by_session():
    # Connect DB
    conn = sqlite3.connect("Database/final_project.db")
    cur = conn.cursor()

    # Query all needed fields in one go
    query = """
        SELECT weather_sessions.session_name,
               flights_data.departure_delay
        FROM flights_data
        JOIN weather_sessions
          ON flights_data.weather_id = weather_sessions.id
    """
    cur.execute(query)
    rows = cur.fetchall()

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



# Function that gets wind_speed & humidity, takes in {str date: int avg_delay}, 
# return {str date: int avg_delay, int wind_speed, int humidity}
    # 

# ========== WRITING INTO FILE ==========




# ========== PLOTTING ==========
# Scatter plot 1:

# Scatter plot 2:


# ========== MAIN FUNCTION ==========
def main():
    avg_delays = get_avg_delay_by_session()
    print(avg_delays)


if __name__ == "__main__":
    main()