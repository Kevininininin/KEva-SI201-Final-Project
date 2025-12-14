# Eva's graphs
    # Bar chart of avg delay for is_day or is_not_day
    # bar chart for windspeed vs airline: does certain airlines experience higher or lower avg windspeed than other airlines?
    
import sqlite3
import matplotlib.pyplot as plt


# ========== CALCULATION & DATA PREP ==========
def get_avg_delay_by_session():
    """
    Get data from final_project_2.db
    Return dict {session_name : avg_delay}
    """

    # Connect DB
    conn = sqlite3.connect("Database/final_project_2.db")
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

    return return_dict


# ========== MAIN FUNCTION ==========
def main():
    # Calculating delay
    avg_delays = get_avg_delay_by_session()
    print(avg_delays)


if __name__ == "__main__":
    main()