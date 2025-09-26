# test_sport_benefits.py
import pytest
from datetime import datetime, timedelta
import pandas as pd
from sport_benefits import validate_activities, SPORT_TYPES

@pytest.fixture
def sample_activities():
    """Generate sample activity data for testing."""
    return pd.DataFrame({
        'ID': [1, 2, 3, 4, 5],
        'ID_salarié': [101, 101, 102, 103, 104],
        'Date_de_début': [
            datetime.now() - timedelta(days=1),
            datetime.now() + timedelta(days=1),  # Future date
            datetime.now() - timedelta(days=2),
            datetime.now() - timedelta(days=3),
            datetime.now() - timedelta(days=4)
        ],
        'Type': ['Vélo', 'Course à pied', 'Escalade', 'Marche', 'Natation'],
        'Distance': [10000, -500, None, 5000, 300],  # Negative distance
        'Date_de_fin': [
            datetime.now() - timedelta(days=1) + timedelta(hours=1),
            datetime.now() + timedelta(days=1, hours=1),
            datetime.now() - timedelta(days=2) + timedelta(hours=2),
            datetime.now() - timedelta(days=3) + timedelta(hours=1),
            datetime.now() - timedelta(days=4) + timedelta(minutes=30)
        ],
        'Commentaire': ['', '', '', '', '']
    })

def test_validate_future_dates(sample_activities):
    results = validate_activities(sample_activities)
    assert 2 in results['future_dates']  # ID 2 has future date
    assert len(results['future_dates']) == 1

def test_validate_negative_distances(sample_activities):
    results = validate_activities(sample_activities)
    assert 2 in results['negative_distances']  # ID 2 has negative distance
    assert len(results['negative_distances']) == 1

def test_validate_sport_distances(sample_activities):
    results = validate_activities(sample_activities)
    # ID 5 has 300m swimming which is below minimum (500m)
    assert 5 in results['invalid_sport_distances']
    assert len(results['invalid_sport_distances']) == 1

def test_valid_activities():
    valid_data = pd.DataFrame({
        'ID': [1],
        'ID_salarié': [101],
        'Date_de_début': [datetime.now() - timedelta(days=1)],
        'Type': ['Vélo'],
        'Distance': [15000],
        'Date_de_fin': [datetime.now() - timedelta(days=1) + timedelta(hours=2)],
        'Commentaire': ['']
    })
    results = validate_activities(valid_data)
    assert not any(results.values())  # All validation lists should be empty
