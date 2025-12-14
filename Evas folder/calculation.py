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



def add_is_day_to_avg_delay(avg_delay_dict):
    # Connect DB
    conn = sqlite3.connect("Database/final_project_2.db")
    cur = conn.cursor()

    # Query weather_sessions table
    query = """
        SELECT session_name, is_day
        FROM weather_sessions
    """
    cur.execute(query)
    rows = cur.fetchall() #list of tupples [(str session_name, int is_day)]

    # Prepare return_dict
    return_dict = {}

    # Enrich avg delay data directly in one loop
    for session_name, is_day in rows:
        if session_name in avg_delay_dict:
            avg_delay = avg_delay_dict[session_name]

            return_dict[session_name] = {
                "avg_delay": avg_delay,
                "is_day": is_day
            }

    # Close DB
    conn.close()

    # Return result
    return return_dict

def compute_daynight_avg(enriched_dict):
    day_delays = []
    night_delays = []

    for session, data in enriched_dict.items():
        if data["is_day"] == 1:
            day_delays.append(data["avg_delay"])
        else:
            night_delays.append(data["avg_delay"])

    # calculate average delays by day and night
    avg_day = sum(day_delays) / len(day_delays)
    avg_night = sum(night_delays) / len(night_delays)

    return {"Day": avg_day, "Night": avg_night}






def get_airline_wind_delay():
    """
    Returns dict:
        { airline_name: { "avg_delay": x, "avg_wind": y } }
    """
    conn = sqlite3.connect("Database/final_project_2.db")
    cur = conn.cursor()

    query = """
        SELECT 
            airlines.name,
            flights_data.departure_delay,
            weather_sessions.wind_speed
        FROM flights_data
        JOIN airlines
            ON flights_data.airline_id = airlines.id
        JOIN weather_sessions
            ON flights_data.weather_id = weather_sessions.id
        WHERE flights_data.departure_delay IS NOT NULL
    """

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    data = {}

    for airline, delay, wind in rows:
        if airline not in data:
            data[airline] = {
                "delay_sum": 0,
                "delay_count": 0,
                "wind_sum": 0,
                "wind_count": 0
            }

        data[airline]["delay_sum"] += delay
        data[airline]["delay_count"] += 1
        data[airline]["wind_sum"] += wind
        data[airline]["wind_count"] += 1

    # Convert sums to avg
    result = {}
    for airline in data:
        avg_delay = data[airline]["delay_sum"] / data[airline]["delay_count"]
        avg_wind = data[airline]["wind_sum"] / data[airline]["wind_count"]
        result[airline] = {
            "avg_delay": avg_delay,
            "avg_wind": avg_wind
        }

    return result






# ========== PLOTTING ==========
# Bar chart 1: avg delay vs. is_day

def plot_daynight_delay(avg_dict):
    labels = list(avg_dict.keys())     # ["Day", "Night"]
    values = list(avg_dict.values())   # [avg_day, avg_night]

    plt.figure()

    # create bar chart
    plt.bar(labels, values, color=["gold", "navy"])

    # add labels and title
    plt.title("Average Flight Delay: Day vs Night")
    plt.ylabel("Average Delay (minutes)")
    plt.xlabel("Time of Day")
    plt.tight_layout()
    # plt.show()

    #Save figure directly to Evas folder
    plt.savefig("Evas folder/delay_vs_is_day.png")
    plt.close()


def plot_wind_vs_delay_per_airline(airline_dict):
    """
    Scatter plot of avg wind vs avg delay per airline.
    """

    # Extract x (wind) and y (delay)
    winds = []
    delays = []
    labels = []

    for airline, metrics in airline_dict.items():
        winds.append(metrics["avg_wind"])
        delays.append(metrics["avg_delay"])
        labels.append(airline)

    plt.figure(figsize=(10, 6))
    plt.scatter(winds, delays)

    # Add labels for each airline (optional)
    for i, name in enumerate(labels):
        plt.annotate(name, (winds[i], delays[i]), fontsize=6, alpha=0.7)

    plt.xlabel("Average Wind Speed (m/s)")
    plt.ylabel("Average Departure Delay (min)")
    plt.title("Correlation: Wind Speed vs Flight Delay per Airline")

    plt.tight_layout()
    plt.savefig("Evas folder/wind_vs_delay_per_airline.png")
    plt.close()




# ========== MAIN FUNCTION ==========
def main():
    # Calculating delay
    avg_delays = get_avg_delay_by_session()
    # print(avg_delays)

    # Collect is_data data to add to avg_delays dict
    avg_delay_enriched= add_is_day_to_avg_delay(avg_delays)
    # print(avg_delay_enriched)

    computed_dict=compute_daynight_avg(avg_delay_enriched)

    plot_daynight_delay(computed_dict)

    airline_stats = get_airline_wind_delay()
    plot_wind_vs_delay_per_airline(airline_stats)


if __name__ == "__main__":
    main()