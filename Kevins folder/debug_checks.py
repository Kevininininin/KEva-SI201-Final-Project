import json

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

def main():
    session_name = "2025_Dec_2_Night"
    start = 1
    end = 3
    debug_print_flights(session_name, start, end)

if __name__ == "__main__":
    main()