import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
from feed_functions.heifers.heifer_cost_model import HeiferCostModel

# feed_functions/heifers/test_heifer_cost_model.py



class TestHeiferCostModel(unittest.TestCase):
    @patch('feed_functions.heifers.heifer_cost_model.pd.read_csv')
    @patch('feed_functions.heifers.heifer_cost_model.Feedcost_basics')
    def test_create_feed_amount_tmr_phase(self, mock_FCB, mock_read_csv):
        # Mock Feedcost_basics
        mock_fcb_instance = MagicMock()
        # Set up current_feedcost with known dry_kg and dry_cost
        mock_fcb_instance.current_feedcost = pd.DataFrame(
            {'dry_kg': [10.0], 'dry_cost': [50.0]}, index=['sum']
        )
        mock_FCB.return_value = mock_fcb_instance

        # Mock heifers.csv
        bd1 = pd.DataFrame({
            'heifer_ids': ['H1'],
            'b_date': [pd.Timestamp('2023-01-01')],
            'adj_bdate': [pd.NaT],
            'ultra_conf_date': [pd.Timestamp('2023-01-10')],
            'calf_bdate': [pd.NaT],
            'arrived': [np.nan]
        })
        mock_read_csv.side_effect = [bd1]

        # Patch create_daily_feedprice and create_heifer_feedcost to avoid file IO
        with patch.object(HeiferCostModel, 'create_daily_feedprice', return_value=pd.DataFrame()), \
             patch.object(HeiferCostModel, 'create_heifer_feedcost', return_value=1.0):
            model = HeiferCostModel()
            # Set up feeding ranges for TMR only
            model.born_here_ids = pd.Series(['H1'])
            today = pd.Timestamp('2023-01-15')
            model.today = today
            model.rng1 = pd.date_range('2023-01-01', today, freq='D')
            # Only TMR phase active
            model.eatingTMR_range_dict = {'H1': (pd.Timestamp('2023-01-10'), pd.Timestamp('2023-01-12'))}
            model.milkdrinking_range_dict = {'H1': pd.DatetimeIndex([])}
            model.eatingHeiferfood_range_dict = {'H1': (None, None)}

            # Run create_feed_amount
            feed_amount_df = model.create_feed_amount()

            # Calculate expected value
            TMR_amount_series = 10.0
            TMR_cost_series = 50.0
            TMR_price_per_kg = TMR_cost_series / TMR_amount_series
            grad_series = np.geomspace(6.0, 30, 730)
            n_days = 3  # 2023-01-10, 2023-01-11, 2023-01-12
            expected_values = grad_series[:n_days] * TMR_price_per_kg

            # Check values for each date
            tmr_dates = pd.date_range('2023-01-10', '2023-01-12')
            for i, date in enumerate(tmr_dates):
                actual = feed_amount_df.loc[date, 'H1']
                expected = expected_values[i]
                self.assertAlmostEqual(actual, expected, places=6)

if __name__ == '__main__':
    unittest.main()