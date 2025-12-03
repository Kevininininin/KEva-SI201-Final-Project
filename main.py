# Name: Jinghan Wu, Eva Park
# UMID: 77132944, _____
# E-mail: kevinwuu@umich.edu, evapark@umich.edu
# AI disclosure: TBD

import json
import os


def load_json(filepath):
    """
    Load a JSON file and return the parsed data.
    """
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {filepath}")
        return None
    except json.JSONDecodeError:
        print(f"[ERROR] JSON decode error in file: {filepath}")
        return None


def display_weather(filename):
    """
    Load and display weather information from a JSON file stored in weather_json_raw_data/.
    
    Expected structure:
    data["location"]["name"]
    data["location"]["localtime"]
    data["current"]["wind_speed"], etc.
    """

    filepath = os.path.join("weather_json_raw_data", filename)

    weather = load_json(filepath)
    if not weather:
        print("[ERROR] Could not load weather data.")
        return

    location = weather.get("location", {})
    current = weather.get("current", {})

    print("\n===== WEATHER DATA =====")
    print(f"City: {location.get('name', 'N/A')}")
    print(f"Local time: {location.get('localtime', 'N/A')}")
    print(f"Wind speed: {current.get('wind_speed', 'N/A')}")
    print(f"Pressure: {current.get('pressure', 'N/A')}")
    print(f"Precipitation: {current.get('precip', 'N/A')}")
    print(f"Humidity: {current.get('humidity', 'N/A')}")
    print(f"Cloudcover: {current.get('cloudcover', 'N/A')}")
    print(f"Visibility: {current.get('visibility', 'N/A')}")
    print("========================\n")



def display_flights(filename, max_flights=5):
    filepath = os.path.join("aviation_json_raw_data", filename)

    data = load_json(filepath)
    if not data:
        print("[ERROR] Could not load flight data.")
        return

    flights = data.get("data", [])
    if not flights:
        print("[INFO] No flights in this file.")
        return

    print("\n=== FLIGHT DATA (W/ Departure Delay) ===")

    shown = 0  # count how many printed
    
    for flight in flights:
        # Stop when we've printed enough
        if shown >= max_flights:
            break

        flight_number = flight.get("flight", {}).get("iata", "N/A")
        delay = flight.get("departure", {}).get("delay", None)

        # Skip flights with no departure delay
        if delay is None:
            continue

        shown += 1

        print(f"\nFlight {shown}")
        print(f"Flight number: {flight_number}")
        print(f"Delay (departure): {delay}")

    if shown == 0:
        print("\n[INFO] No delayed flights found.")

    print("\n=======================\n")




def main():
    display_weather("weather_2025_Dec_2_Night.json")
    display_flights(filename = "flights_2025_Dec_2_Night.json", max_flights = 100)


if __name__ == "__main__":
    main()
