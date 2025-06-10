import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from airports import INDIAN_AIRPORTS


class FlightDataCollector:
    def __init__(self, flightaware_key, weather_key):
        self.flightaware_base = "https://flightxml.flightaware.com/json/FlightXML3/"
        self.flightaware_headers = {"x-apikey": flightaware_key}
        self.weather_api = weather_key
        self.weather_cache = {}

    def get_route_weather(self, origin, destination, flight_time):
        """Get historical weather along route using OpenWeatherMap API"""
        cache_key = f"{origin}-{destination}-{flight_time.date()}"
        if cache_key in self.weather_cache:
            return self.weather_cache[cache_key]

        try:
            # Get midpoint weather (simplified - in reality would need proper routing)
            response = requests.get(
                "https://api.openweathermap.org/data/3.0/onecall/timemachine",
                params={
                    "lat": self._get_midpoint(origin, destination)['lat'],
                    "lon": self._get_midpoint(origin, destination)['lon'],
                    "dt": int(flight_time.timestamp()),
                    "appid": self.weather_api,
                    "units": "metric"
                }
            )
            weather_data = response.json()
            self.weather_cache[cache_key] = weather_data
            return weather_data
        except Exception as e:
            print(f"Weather API error: {e}")
            return None

    def _get_midpoint(self, origin_icao, dest_icao):
        """Calculate approximate midpoint between airports"""
        origin = INDIAN_AIRPORTS.get(origin_icao, {})
        dest = INDIAN_AIRPORTS.get(dest_icao, {})

        # Simple midpoint calculation (for demo - real routing would use Great Circle)
        return {
            'lat': (origin.get('lat', 0) + dest.get('lat', 0)) / 2,
            'lon': (origin.get('lon', 0) + dest.get('lon', 0)) / 2
        }

    def get_flight_details(self, flight_id):
        """Get detailed flight trajectory and timing"""
        try:
            response = requests.get(
                f"{self.flightaware_base}FlightInfoStatus",
                params={"ident": flight_id, "include_ex_data": 1},
                headers=self.flightaware_headers
            )
            return response.json().get("FlightInfoStatusResult", {})
        except Exception as e:
            print(f"Error getting flight details: {e}")
            return None

    def process_flight(self, flight, airport_icao):
        try:
            # Get detailed flight data
            details = self.get_flight_details(flight['ident'])
            if not details:
                return None

            # Calculate en-route weather
            departure_time = datetime.fromtimestamp(details.get('filed_departuretime', 0))
            arrival_time = datetime.fromtimestamp(flight['actualarrivaltime'])
            route_weather = self.get_route_weather(
                details.get('origin'),
                airport_icao,
                departure_time + (arrival_time - departure_time) / 2
            )

            return {
                "flight_number": flight['ident'],
                "airline": flight.get('operator', 'Unknown'),
                "origin": details.get('origin'),
                "destination": airport_icao,
                "scheduled_departure": departure_time,
                "actual_departure": datetime.fromtimestamp(details.get('actualdeparturetime', 0)),
                "scheduled_arrival": datetime.fromtimestamp(flight['estimatedarrivaltime']),
                "actual_arrival": datetime.fromtimestamp(flight['actualarrivaltime']),
                "delay_minutes": (flight['actualarrivaltime'] - flight['estimatedarrivaltime']) / 60,
                "aircraft": flight.get('aircrafttype', 'Unknown'),
                "route_weather": route_weather['current']['weather'][0]['main'] if route_weather else 'Unknown',
                "route_weather_desc": route_weather['current']['weather'][0][
                    'description'] if route_weather else 'Unknown',
                "route_temp": route_weather['current']['temp'] if route_weather else None,
                "route_wind": route_weather['current']['wind_speed'] if route_weather else None
            }
        except Exception as e:
            print(f"Error processing flight {flight.get('ident')}: {e}")
            return None

    def collect_data(self, hours=24, filename="flight_data_enhanced.xlsx"):
        all_flights = []

        for icao in INDIAN_AIRPORTS:
            print(f"Processing {icao}...")
            try:
                # Get airport board
                response = requests.get(
                    f"{self.flightaware_base}AirportBoards",
                    params={
                        "airport": icao,
                        "howMany": 10,
                        "startTime": int((datetime.now() - timedelta(hours=hours)).timestamp()),
                        "endTime": int(datetime.now().timestamp())
                    },
                    headers=self.flightaware_headers
                )
                flights = response.json().get("AirportBoardsResult", {}).get("arrivals", {}).get("flights", [])

                # Process each flight
                for flight in flights:
                    processed = self.process_flight(flight, icao)
                    if processed:
                        all_flights.append(processed)

                time.sleep(2)  # Rate limiting
            except Exception as e:
                print(f"Error processing airport {icao}: {e}")
                continue

        # Save to Excel
        df = pd.DataFrame(all_flights)
        df.to_excel(filename, index=False)
        print(f"Data saved to {filename}")
        return df