"""
groups_and_tests.lactation_predictor.py

Spline-based lactation prediction model for ascent and descent.
Fits a smoothing spline to weekly liters per cow and predicts future values.
Diagnostics include observed and predicted peak yield, peak day, and persistency proxy.
"""

import numpy as np
import pandas as pd
from container import get_dependency
from scipy.interpolate import UnivariateSpline
from typing import Tuple, Dict, Any


class SplineLactationPredictor_weekly:
    def __init__(self):
        self.LLS = None
        self.m1_weekly: pd.DataFrame | None = None
        self.max_weekly: pd.Series | None = None
        self.predictions_liters: pd.DataFrame | None = None
        self.params: pd.DataFrame | None = None

    def load_and_process(self, future_periods: int = 5, spline_s: float = None) -> None:
        self.LLS = get_dependency('lactations_log_standard')
        self.m1_weekly = self.LLS.m1_weekly
        self.max_weekly = self.LLS.max_weekly
        self.predictions_liters, self.params = self.fit_and_predict_all(
            self.m1_weekly, future_periods=future_periods, spline_s=spline_s
        )

    @staticmethod
    def apply_post_peak_decline(values: np.ndarray, peak_idx: int, decline_rate: float = 0.98) -> np.ndarray:
        """
        Apply a fixed percent decline after the peak index.
        """
        out = values.copy()
        for i in range(peak_idx + 1, len(out)):
            out[i] = out[i - 1] * decline_rate
        return out

    @staticmethod
    def fit_spline_to_cow(
        cow_series: pd.Series, future_periods: int = 5, spline_s: float = None, decline_rate: float = 0.98
    ) -> Tuple[np.ndarray, Dict[str, float]]:
        n_obs = len(cow_series)
        k = 3
        if n_obs < k + 1:
            future_preds = np.full(future_periods, np.nan)
            diagnostics = {"note": f"not enough observations (n={n_obs})"}
            return future_preds, diagnostics

        x = np.arange(1, n_obs + 1)
        y = cow_series.values
        future_x = np.arange(n_obs + 1, n_obs + future_periods + 1)

        spline = UnivariateSpline(x, y, s=spline_s)
        spline_y = spline(x)
        peak_idx = int(np.nanargmax(spline_y))
        # Apply post-peak decline to fitted values
        spline_y_declined = SplineLactationPredictor_weekly.apply_post_peak_decline(spline_y, peak_idx, decline_rate)
        # Predict future values by continuing the decline
        last_val = spline_y_declined[-1]
        future_preds = np.array([last_val * (decline_rate ** (i + 1)) for i in range(future_periods)])
        future_preds = np.clip(future_preds, 0.0, None)

        observed_peak_idx = int(np.nanargmax(y))
        observed_peak_yield = float(np.nanmax(y))
        fitted_peak_idx = peak_idx
        fitted_peak_yield = float(spline_y_declined[peak_idx])

        # Calculate percent decline per week after peak (persistency)
        if fitted_peak_idx < len(spline_y_declined) - 1:
            post_peak = spline_y_declined[fitted_peak_idx:]
            persistency = np.mean(post_peak[1:] / post_peak[:-1])
        else:
            persistency = np.nan

        diagnostics = {
            "observed_peak_day": observed_peak_idx + 1,
            "observed_peak_yield": observed_peak_yield,
            "fitted_peak_day": fitted_peak_idx + 1,
            "fitted_peak_yield": fitted_peak_yield,
            "persistency_proxy": persistency,
            "spline_s": spline_s if spline_s is not None else "auto"
        }

        return future_preds, diagnostics

    def fit_and_predict_all(
        self, weekly_liters: pd.DataFrame, future_periods: int = 5, spline_s: float = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fit smoothing spline per cow to weekly_liters DataFrame and return:
        - extended_weekly: DataFrame with observed + predicted values, padded to same length
        - params_df: DataFrame with diagnostics per cow
        """
        cow_arrays = {}
        params: Dict[str, Any] = {}
        max_len = 0

        for cow in weekly_liters.columns:
            cow_series = weekly_liters[cow].dropna()
            preds, diagnostics = self.fit_spline_to_cow(
                cow_series, future_periods=future_periods, spline_s=spline_s
            )
            full_array = np.concatenate([cow_series.values, preds])
            cow_arrays[cow] = full_array
            max_len = max(max_len, len(full_array))
            params[cow] = diagnostics

        # Pad arrays to max_len
        for cow, arr in cow_arrays.items():
            if len(arr) < max_len:
                arr = np.pad(arr, (0, max_len - len(arr)), constant_values=np.nan)
            cow_arrays[cow] = arr

        extended_weekly = pd.DataFrame(cow_arrays)
        params_df = pd.DataFrame(params).T
        cols_order = [
            "observed_peak_day", "observed_peak_yield",
            "fitted_peak_day", "fitted_peak_yield",
            "persistency_proxy", "spline_s"
        ]
        params_df = params_df[[c for c in cols_order if c in params_df.columns]]
        
        extended_weekly.to_csv("F:\\COWS\\data\\milk_data\\tests\\spline_extended.csv")
        return extended_weekly, params_df


if __name__ == '__main__':
    predictor = SplineLactationPredictor_weekly()
    predictor.load_and_process(future_periods=5)
    predictor.predictions_liters.to_csv("F:\\COWS\\data\\milk_data\\tests\\spline_test_weekly.csv")