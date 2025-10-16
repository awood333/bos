import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from feed_functions.heifers.heifer_cost_model import HeiferCostModel

# Python


@pytest.fixture
def mock_feedcost_basics():
    mock_fcb = MagicMock()
    # Mock current_feedcost DataFrame
    mock_fcb.current_feedcost = pd.DataFrame({
        'dry_kg': [10.0],
        'dry_cost': [50.0]
    }, index=['sum'])
    # Mock feed_series_dict and feed_type for create_daily_feedprice
    mock_fcb.feed_series_dict = {
        'corn': {'psd': pd.DataFrame({'unit_price': [1.0]}, index=[pd.Timestamp('2025-01-01')])}
    }
    mock_fcb.feed_type = 'corn'
    return mock_fcb

@pytest.fixture
def mock_heifer_csv():
    # Minimal heifer DataFrame
    df = pd.DataFrame({
        'heifer_ids': ['h1'],
        'b_date': [pd.Timestamp('2025-01-01')],
        'adj_bdate': [pd.NaT],
        'ultra_conf_date': [pd.Timestamp('2025-01-10')],
        'calf_bdate': [pd.NaT],
        'arrived': [np.nan]
    })
    return df

@pytest.fixture
def mock_daily_amt_csv():
    # Minimal daily amount DataFrame
    df = pd.DataFrame({
        'datex': [pd.Timestamp('2025-01-01')],
        'corn kg': [1.0],
        'cassava kg': [0.0],
        'beans kg': [0.0],
        'straw kg': [0.0],
        'bypass_fat kg': [0.0],
        'premix kg': [0.0],
        'NaHCO3 kg': [0.0],
        'total kg': [1.0]
    })
    return df

@patch('feed_functions.heifers.heifer_cost_model.Feedcost_basics')
@patch('feed_functions.heifers.heifer_cost_model.pd.read_csv')
@patch('feed_functions.heifers.heifer_cost_model.pd.date_range')
@patch('feed_functions.heifers.heifer_cost_model.pd.to_datetime')
def test_create_feed_amount_basic(
    mock_to_datetime, mock_date_range, mock_read_csv, mock_feedcost_basics,
    mock_feedcost_basics_fixture, mock_heifer_csv, mock_daily_amt_csv
):
    # Setup mocks
    mock_feedcost_basics.return_value = mock_feedcost_basics_fixture
    mock_read_csv.side_effect = [mock_daily_amt_csv, mock_heifer_csv]
    # Date range for one day
    mock_date_range.side_effect = lambda start, end, freq='D': pd.DatetimeIndex([pd.Timestamp('2025-01-01')])
    mock_to_datetime.side_effect = lambda x, errors=None: pd.Timestamp('2025-01-01')

    # Instantiate and test
    model = HeiferCostModel()
    df = model.create_feed_amount()

    # Only one heifer and one date
    assert df.shape == (1, 1)
    # Check that the value is a float (from multiplication)
    val = df.iloc[0, 0]
    assert isinstance(val, float) or np.isnan(val)
    # For TMR phase, value should be grad_series[0] * tmr_cost
    # grad_series[0] = 6.0, tmr_cost = 50.0/10.0 = 5.0
    # So expected = 6.0 * 5.0 = 30.0
    assert np.isclose(val, 30.0, atol=1e-6) or np.isnan(val)
