# sport_benefits.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict, Any

# Sport types configuration
SPORT_TYPES = {
    'Course à pied': {'has_distance': True, 'distance_range': (2000, 25000), 'speed_range': (2.5, 4.5)},
    'Vélo': {'has_distance': True, 'distance_range': (5000, 50000), 'speed_range': (4.0, 10.0)},
    'Marche': {'has_distance': True, 'distance_range': (1000, 15000), 'speed_range': (1.0, 1.8)},
    'Randonnée': {'has_distance': True, 'distance_range': (3000, 30000), 'speed_range': (0.8, 1.5)},
    'Trottinette': {'has_distance': True, 'distance_range': (2000, 20000), 'speed_range': (3.0, 6.0)},
    'Natation': {'has_distance': True, 'distance_range': (500, 3000), 'speed_range': (0.5, 1.5)},
    'Escalade': {'has_distance': False},
    'Yoga': {'has_distance': False},
    'Musculation': {'has_distance': False}
}

def validate_activities(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate activities dataframe against business rules."""
    results = {
        'future_dates': [],
        'negative_distances': [],
        'invalid_sport_distances': []
    }
    
    # Convert dates if needed
    if not pd.api.types.is_datetime64_any_dtype(df['Date_de_début']):
        df['Date_de_début'] = pd.to_datetime(df['Date_de_début'])
    if not pd.api.types.is_datetime64_any_dtype(df['Date_de_fin']):
        df['Date_de_fin'] = pd.to_datetime(df['Date_de_fin'])
    
    # Check for future dates
    now = datetime.now()
    future_mask = df['Date_de_début'] > now
    if future_mask.any():
        results['future_dates'] = df[future_mask]['ID'].tolist()
    
    # Check negative distances for sports that should have distance
    if 'Distance' in df.columns:
        distance_sports = [k for k, v in SPORT_TYPES.items() if v['has_distance']]
        distance_mask = (df['Type'].isin(distance_sports)) & (df['Distance'].fillna(-1) < 0)
        if distance_mask.any():
            results['negative_distances'] = df[distance_mask]['ID'].tolist()
    
    # Check sport-specific distance ranges
    invalid_distances = []
    for sport, config in SPORT_TYPES.items():
        if config['has_distance']:
            sport_mask = df['Type'] == sport
            min_dist, max_dist = config['distance_range']
            invalid_mask = sport_mask & (
                (df['Distance'] < min_dist) | 
                (df['Distance'] > max_dist))
            invalid_distances.extend(df[invalid_mask]['ID'].tolist())
    results['invalid_sport_distances'] = invalid_distances
    
    return results
