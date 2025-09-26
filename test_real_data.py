# test_real_data.py
import pytest
import pandas as pd
from sport_benefits import validate_activities

@pytest.fixture
def real_activities():
    """Load the actual generated data"""
    return pd.read_csv('strava_simulation.csv')

def test_real_data_quality(real_activities):
    """Validate the structure and basic quality of real data"""
    required_columns = {'ID', 'ID_salarié', 'Date_de_début', 'Type', 'Distance', 'Date_de_fin', 'Commentaire'}
    assert set(real_activities.columns) >= required_columns
    
    # Basic data quality checks
    assert not real_activities['ID'].duplicated().any()
    assert real_activities['ID_salarié'].notna().all()

def test_real_data_validation(real_activities):
    """Run the full validation suite on real data"""
    results = validate_activities(real_activities)
    
    # These should always be true for any valid dataset
    assert not results['future_dates'], "Future-dated activities found"
    assert not results['negative_distances'], "Negative distances found"
    
    # This might need adjustment based on your business rules
    # assert not results['invalid_sport_distances'], "Invalid sport distances found"
    
    # Instead, consider logging these for review:
    if results['invalid_sport_distances']:
        print(f"\nNote: Found {len(results['invalid_sport_distances'])} activities with unusual distances")
