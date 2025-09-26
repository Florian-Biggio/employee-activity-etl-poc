import json

try:
    with open("secret.json") as f:
        secrets = json.load(f)
    API_KEY = secrets.get("GOOGLE_API_KEY")
    if not API_KEY:
        raise ValueError("API key not found in secret.json")
except FileNotFoundError:
    raise FileNotFoundError("secret.json not found. Please add your API key.")

company_Adress = "1362 Av. des Platanes, 34970 Lattes"
employee_Adress = "128 Rue du Port, 34000 Frontignan"
employee_Adress2 = "74 Rue des Fleurs, 34970 Lattes"

import googlemaps
from datetime import datetime
from typing import Optional, Dict, List, Tuple
import functools

# Cache decorator for API calls
def cache_results(func):
    """Simple memoization decorator to cache API responses"""
    cache = {}
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        cache_key = str(args) + str(kwargs)
        if cache_key not in cache:
            cache[cache_key] = func(*args, **kwargs)
        return cache[cache_key]
    return wrapper

@cache_results
def geocode_address(client: googlemaps.Client, address: str) -> Optional[Tuple[float, float]]:
    """Convert address to coordinates using Geocoding API"""
    try:
        result = client.geocode(address)
        if result:
            location = result[0]['geometry']['location']
            return (location['lat'], location['lng'])
    except Exception as e:
        print(f"Geocoding failed for '{address}': {str(e)}")
    return None

def calculate_route(
    client: googlemaps.Client,
    origin: str,
    destination: str,
    mode: str = "driving",
    alternatives: bool = False,
    units: str = "metric"
) -> Optional[Dict]:
    """Calculate route between two locations using Routes API"""
    try:
        directions = client.directions(
            origin,
            destination,
            mode=mode,
            departure_time=datetime.now(),
            alternatives=alternatives,
            units=units
        )
        
        if not directions:
            return None
            
        primary_route = directions[0]
        leg = primary_route['legs'][0]
        
        result = {
            "origin": leg['start_address'],
            "destination": leg['end_address'],
            "distance": leg['distance']['text'],
            "duration": leg['duration']['text'],
            "travel_mode": mode.upper(),
            "steps": [{
                "instruction": step['html_instructions'],
                "distance": step['distance']['text'],
                "duration": step['duration']['text']
            } for step in leg['steps']]
        }
        
        if alternatives and len(directions) > 1:
            result["alternatives"] = [{
                "summary": alt['summary'],
                "distance": alt['legs'][0]['distance']['text'],
                "duration": alt['legs'][0]['duration']['text']
            } for alt in directions[1:]]
        
        return result
        
    except Exception as e:
        print(f"Route calculation failed: {str(e)}")
        return None

def get_route_between_addresses(
    client: googlemaps.Client,
    origin_address: str,
    destination_address: str,
    **kwargs
) -> Optional[Dict]:
    """Higher-level function that handles geocoding and routing"""
    origin_coords = geocode_address(client, origin_address)
    destination_coords = geocode_address(client, destination_address)
    
    if not origin_coords or not destination_coords:
        return None
        
    return calculate_route(
        client,
        origin=origin_coords,
        destination=destination_coords,
        **kwargs
    )

# Example usage
if __name__ == "__main__":
    # Initialize client once
    client = googlemaps.Client(key=API_KEY)
    
    # Example 1: Direct route calculation
    route = calculate_route(
        client,
        origin=employee_Adress,
        destination=company_Adress,
        mode="walking"
    )
    
    if route:
        print(f"Route from {route['origin']} to {route['destination']}")
        print(f"Distance: {route['distance']}")
        print(f"Duration: {route['duration']}")
        print(f"First step: {route['steps'][0]['instruction']}")
    
    company_Adress = "1362 Av. des Platanes, 34970 Lattes"
    employee_Adress = "128 Rue du Port, 34000 Frontignan"
    employee_Adress2 = "74 Rue des Fleurs, 34970 Lattes"

    # Example 2: With automatic geocoding
    route = get_route_between_addresses(
        client,
        origin_address=employee_Adress,
        destination_address=company_Adress,
        mode="driving",
        alternatives=True
    )
    
    if route:
        print(f"\nDriving route from {route['origin']} to {route['destination']}")
        print(f"Distance: {route['distance']}")
        print(f"Duration: {route['duration']}")
        if "alternatives" in route:
            print(f"\nFound {len(route['alternatives'])} alternative routes:")
            for i, alt in enumerate(route['alternatives'], 1):
                print(f"{i}. {alt['summary']} ({alt['distance']}, {alt['duration']})")