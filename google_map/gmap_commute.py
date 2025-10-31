import pandas as pd
import numpy as np
import googlemaps
from datetime import datetime, timedelta, time as dt_time
from typing import Optional, Dict, List, Tuple
import functools
import json
import time
import random
from tqdm import tqdm

# === CONFIGURATION ===
try:
    with open("secret.json") as f:
        secrets = json.load(f)
    API_KEY = secrets.get("GOOGLE_API_KEY")
    if not API_KEY:
        raise ValueError("API key not found in secret.json")
except FileNotFoundError:
    raise FileNotFoundError("secret.json not found.")

company_address = "1362 Av. des Platanes, 34970 Lattes"

# === UTILITY FUNCTIONS ===
def cache_results(func):
    cache = {}
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key_args = args[1:]
        cache_key = str(key_args) + str(sorted(kwargs.items()))
        if cache_key not in cache:
            cache[cache_key] = func(*args, **kwargs)
        return cache[cache_key]
    return wrapper

@cache_results
def geocode_address(client: googlemaps.Client, address: str) -> Optional[Tuple[float, float]]:
    try:
        result = client.geocode(address)
        if result:
            location = result[0]['geometry']['location']
            return (location['lat'], location['lng'])
    except Exception as e:
        print(f"Geocoding failed for '{address}': {str(e)}")
    return None

def get_next_weekday(target_weekday=0, hour=10, minute=0):
    """Get next specific weekday at specified time"""
    today = datetime.now()
    days_ahead = (target_weekday - today.weekday() + 7) % 7
    if days_ahead == 0 and today.time() > dt_time(hour, minute):
        days_ahead = 7
    
    target_date = today + timedelta(days=days_ahead)
    return target_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

def calculate_route_with_options(
    client: googlemaps.Client,
    origin: str,
    destination: str,
    mode: str = "driving",
    departure_time: Optional[datetime] = None,
    traffic_model: Optional[str] = None
) -> Optional[Dict]:
    """Calculate route with specific timing options"""
    try:
        api_params = {
            'origin': origin,
            'destination': destination,
            'mode': mode,
            'units': 'metric'
        }
        
        if departure_time:
            api_params['departure_time'] = departure_time
            if mode == 'driving' and traffic_model:
                api_params['traffic_model'] = traffic_model
        
        directions = client.directions(**api_params)
        
        if not directions:
            return None
            
        leg = directions[0]['legs'][0]
        duration_data = leg.get('duration_in_traffic', leg['duration'])
        
        return {
            "duration": duration_data['text'],
            "duration_seconds": duration_data['value'],
            "distance_meters": leg['distance']['value'],
            "distance_text": leg['distance']['text'],
            "has_traffic_data": 'duration_in_traffic' in leg,
            "departure_time": departure_time.isoformat() if departure_time else "current"
        }
        
    except Exception as e:
        print(f"Route calculation failed: {str(e)}")
        return None

def get_meaningful_route_times(
    client: googlemaps.Client,
    origin_address: str,
    destination_address: str,
    mode: str = "driving"
) -> Dict:
    """Get three meaningful time estimates"""
    
    origin_coords = geocode_address(client, origin_address)
    destination_coords = geocode_address(client, destination_address)
    
    if not origin_coords or not destination_coords:
        return {"error": "Geocoding failed"}
    
    results = {}
    
    # 1. TYPICAL CONDITIONS (Monday 10:00 AM - off-peak)
    monday_10am = get_next_weekday(0, 10, 0)  # Monday 10:00 AM
    
    typical_route = calculate_route_with_options(
        client=client,
        origin=origin_coords,
        destination=destination_coords,
        mode=mode,
        departure_time=monday_10am,
        traffic_model="optimistic" if mode == "driving" else None
    )
    
    if typical_route:
        results['typical'] = {
            'duration': typical_route['duration'],
            'duration_seconds': typical_route['duration_seconds'],
            'distance_km': typical_route['distance_meters'] / 1000,
            'distance_text': typical_route['distance_text'],
            'description': 'Typical conditions (Monday 10:00 AM)'
        }
    
    # 2. RUSH HOUR (Tomorrow 8:00 AM)
    tomorrow_8am = (datetime.now() + timedelta(days=1)).replace(hour=8, minute=0, second=0)
    
    rush_hour_route = calculate_route_with_options(
        client=client,
        origin=origin_coords,
        destination=destination_coords,
        mode=mode,
        departure_time=tomorrow_8am,
        traffic_model="best_guess" if mode == "driving" else None
    )
    
    if rush_hour_route:
        results['rush_hour'] = {
            'duration': rush_hour_route['duration'],
            'duration_seconds': rush_hour_route['duration_seconds'],
            'distance_km': rush_hour_route['distance_meters'] / 1000,
            'distance_text': rush_hour_route['distance_text'],
            'description': 'Rush hour (8:00 AM)'
        }
    
    # current_route = calculate_route_with_options(
    #     client=client,
    #     origin=origin_coords,
    #     destination=destination_coords,
    #     mode=mode,
    #     departure_time=datetime.now()
    # )
    
    # if current_route:
    #     results['current'] = {
    #         'duration': current_route['duration'],
    #         'duration_seconds': current_route['duration_seconds'],
    #         'distance_km': current_route['distance_meters'] / 1000,
    #         'distance_text': current_route['distance_text'],
    #         'description': 'Current conditions'
    #     }
    
    return results

def parse_duration_from_seconds(total_seconds: int) -> str:
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def get_employee_commute_corrected(row: pd.Series, company_addr: str) -> Dict:
    transport_mapping = {
        'Transports en commun': 'transit',
        'véhicule thermique/électrique': 'driving',
        'Marche/running': 'walking',
        'Vélo/Trottinette/Autres': 'bicycling'
    }
    
    try:
        employee_name = f"{row.get('Prénom', 'Unknown')} {row.get('Nom', 'Unknown')}"
        home_address = row.get('Adresse du domicile', '')
        
        if not home_address:
            return {
                'Distance_km_typical': None,
                'Duree_hhmmss_typical': "00:00:00",
                'Distance_text_typical': "",
                'Distance_km_rush_hour': None,
                'Duree_hhmmss_rush_hour': "00:00:00",
                'Distance_text_rush_hour': "",
                # 'Distance_km_current': None,
                # 'Duree_hhmmss_current': "00:00:00",
                # 'Distance_text_current': "",
                'Success': False
            }
        
        mode = transport_mapping.get(row.get('Moyen de déplacement', ''), 'driving')
        
        print(f"📍 Calculating routes for {employee_name} ({mode})...")
        
        route_times = get_meaningful_route_times(
            client=client,
            origin_address=home_address,
            destination_address=company_addr,
            mode=mode
        )
        
        if 'error' in route_times:
            return {
                'Distance_km_typical': None,
                'Duree_hhmmss_typical': "00:00:00",
                'Distance_text_typical': "",
                'Distance_km_rush_hour': None,
                'Duree_hhmmss_rush_hour': "00:00:00",
                'Distance_text_rush_hour': "",
                # 'Distance_km_current': None,
                # 'Duree_hhmmss_current': "00:00:00",
                # 'Distance_text_current': "",
                'Success': False
            }
        
        # Extract results
        typical_data = route_times.get('typical', {})
        rush_hour_data = route_times.get('rush_hour', {})
        # current_data = route_times.get('current', {})
        
        result = {
            'Distance_km_typical': typical_data.get('distance_km'),
            'Duree_hhmmss_typical': parse_duration_from_seconds(typical_data.get('duration_seconds', 0)),
            'Distance_text_typical': typical_data.get('distance_text', ''),
            'Distance_km_rush_hour': rush_hour_data.get('distance_km'),
            'Duree_hhmmss_rush_hour': parse_duration_from_seconds(rush_hour_data.get('duration_seconds', 0)),
            'Distance_text_rush_hour': rush_hour_data.get('distance_text', ''),
            # 'Distance_km_current': current_data.get('distance_km'),
            # 'Duree_hhmmss_current': parse_duration_from_seconds(current_data.get('duration_seconds', 0)),
            # 'Distance_text_current': current_data.get('distance_text', ''),
            'Success': True
        }
        
        # Print results
        if typical_data and rush_hour_data:
            print(f"✅ {employee_name}:")
            print(f"   Typical: {typical_data.get('duration')} - {typical_data.get('distance_text')}")
            print(f"   Rush hour: {rush_hour_data.get('duration')} - {rush_hour_data.get('distance_text')}")
            # print(f"   Current: {current_data.get('duration')} - {current_data.get('distance_text')}")
        
        return result
            
    except Exception as e:
        print(f"❌ Error processing {employee_name}: {str(e)}")
        return {
            'Distance_km_typical': None,
            'Duree_hhmmss_typical': "00:00:00",
            'Distance_text_typical': "",
            'Distance_km_rush_hour': None,
            'Duree_hhmmss_rush_hour': "00:00:00",
            'Distance_text_rush_hour': "",
            # 'Distance_km_current': None,
            # 'Duree_hhmmss_current': "00:00:00",
            # 'Distance_text_current': "",
            'Success': False
        }

# === TEST AND MAIN ===
def test_audrey_colin():
    """Test specifically for Audrey Colin to understand the 43 vs 47 minute difference"""
    print("🧪 Testing Audrey Colin's commute...")
    
    global client
    client = googlemaps.Client(key=API_KEY)
    
    test_address = "128 Rue du Port, 34000 Frontignan"
    
    route_times = get_meaningful_route_times(
        client=client,
        origin_address=test_address,
        destination_address=company_address,
        mode="transit"
    )
    
    for time_type, data in route_times.items():
        if time_type != 'error':
            print(f"\n{time_type.upper()}: {data.get('duration')} - {data.get('distance_text')}")
            print(f"Description: {data.get('description')}")

def main():
    # Load data
    rh_path = "../data/DonneesRH.xlsx"
    df_rh = pd.read_excel(rh_path)
    
    global client
    client = googlemaps.Client(key=API_KEY)
    
    print(f"👥 Processing {len(df_rh)} employees...")
    print("🕐 Calculating: Typical conditions + Rush hour")
    print("📏 Showing: Duration + Distance")
    print("-" * 60)
    
    results = []
    for idx, row in tqdm(df_rh.iterrows(), total=len(df_rh), desc="Calculating commutes"):
        commute_data = get_employee_commute_corrected(row, company_address)
        results.append(commute_data)
        time.sleep(random.uniform(2.0, 3.0))
    
    # Add to DataFrame
    for col in ['typical', 'rush_hour']:
        df_rh[f'Distance_km_{col}'] = [r[f'Distance_km_{col}'] for r in results]
        df_rh[f'Duree_hhmmss_{col}'] = [r[f'Duree_hhmmss_{col}'] for r in results]
        df_rh[f'Distance_text_{col}'] = [r[f'Distance_text_{col}'] for r in results]
    df_rh['Commute_Success'] = [r['Success'] for r in results]
    
    # Export
    output_file = "employee_commutes_two_times.csv"
    df_rh.to_csv(output_file, index=False)
    
    print(f"\n🎉 Done! File saved as: {output_file}")
    
    # Show Audrey Colin's results
    audrey = df_rh[(df_rh['Nom'] == 'Colin') & (df_rh['Prénom'] == 'Audrey')]
    if not audrey.empty:
        print(f"\n👤 Audrey Colin's results:")
        print(f"Typical: {audrey['Duree_hhmmss_typical'].iloc[0]} - {audrey['Distance_text_typical'].iloc[0]}")
        print(f"Rush hour: {audrey['Duree_hhmmss_rush_hour'].iloc[0]} - {audrey['Distance_text_rush_hour'].iloc[0]}")
        #print(f"Current: {audrey['Duree_hhmmss_current'].iloc[0]} - {audrey['Distance_text_current'].iloc[0]}")

if __name__ == "__main__":
    # Test Audrey Colin first to see which time matches your original 43 minutes
    # test_audrey_colin()
    
    # Then run main processing
    main()