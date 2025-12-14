# Kevin's graphs
    # Scatter plot comparing avg delay vs. wind speed
    # Scatter plot comparing avg delay vs. humidity 

import sqlite3
import matplotlib.pyplot as plt


# ========== CALCULATION & DATA PREP ==========
def get_avg_delay_by_session():
    """
    Get data from final_project.db
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
    conn = sqlite3.connect("Database/final_project_2.db")
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
def write_calc_summary(enriched_avg_delay_dict):
    """
    Write a summary .txt file to Kevins folder/.
    """

    with open("Kevins folder/calc_summary.txt", "w") as file:
        # Title / header
        file.write("Flight Delay Summary by Weather Session\n")
        file.write("This file summarizes average flight delays along with wind speed and "
                   "humidity for flights departing from JFK Airport in New York City.\n\n")

        # Write each session's data
        for session_name, data in enriched_avg_delay_dict.items():
            avg_delay = data["avg_delay"]
            wind_speed = data["wind_speed"]
            humidity = data["humidity"]

            file.write(f"{session_name}:\n")
            file.write(f"  Avg delay is {avg_delay} min\n")
            file.write(f"  Wind speed is {wind_speed} m/s\n")
            file.write(f"  Humidity is {humidity}%\n\n")


# ========== PLOTTING ==========
# Scatter plot 1: avg delay vs. wind speed
def plot_delay_vs_wind(enriched_dict):
    """
    Creates and saves a scatter plot of:
    X-axis: wind speed
    Y-axis: average flight delay
    """

    # Extract x and y values
    wind_speeds = []
    avg_delays = []

    for date, metrics in enriched_dict.items():
        wind_speeds.append(metrics["wind_speed"])
        avg_delays.append(metrics["avg_delay"])

    # Create scatter plot
    plt.figure()
    plt.scatter(wind_speeds, avg_delays, color="red")

    # Add labels and title
    plt.xlabel("Wind Speed (m/s)")
    plt.ylabel("Average Flight Delay (min)")
    plt.title("Average Flight Delay vs Wind Speed")

    # Save figure directly to Kevins folder
    plt.savefig("Kevins folder/delay_vs_wind_scatter.png")

    # Show plot
    # plt.show()

    # Close figure
    plt.close()


# Scatter plot 2: avg delay vs. humidity
def plot_delay_vs_humidity(enriched_dict):
    """
    Creates and saves a scatter plot of:
    X-axis: humidity
    Y-axis: average flight delay
    """

    # Extract x and y values
    humidities = []
    avg_delays = []

    for date, metrics in enriched_dict.items():
        humidities.append(metrics["humidity"])
        avg_delays.append(metrics["avg_delay"])

    # Create scatter plot
    plt.figure()
    plt.scatter(humidities, avg_delays, color="green")

    # Add labels and title
    plt.xlabel("Humidity (%)")
    plt.ylabel("Average Flight Delay (min)")
    plt.title("Average Flight Delay vs Humidity")

    # Save figure directly to Kevins folder
    plt.savefig("Kevins folder/delay_vs_humidity_scatter.png")

    # Show plot
    # plt.show()

    # Close figure
    plt.close()


# ========== MAIN FUNCTION ==========
def main():
    # Calculating delay
    avg_delays = get_avg_delay_by_session()

    # Collect wind and humidity data to add to avg_delays dict
    avg_delay_enriched = add_weather_to_avg_delay(avg_delays)
    
    # for key, value in avg_delay_enriched.items():
    #     print(f"{key} : {value}")

    # Write calculation summary into text file
    write_calc_summary(avg_delay_enriched)

    # Plot scattered plots
    plot_delay_vs_wind(avg_delay_enriched)
    plot_delay_vs_humidity(avg_delay_enriched)


if __name__ == "__main__":
    main()