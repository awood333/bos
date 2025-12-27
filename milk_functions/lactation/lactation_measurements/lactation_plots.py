'''milk_functions//lactation//lactation_measurements//lactation_plots.py'''
import numpy as np
import base64
import io
import matplotlib.pyplot as plt
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from container import get_dependency

class LactationPlots:
    def __init__(self):
        self.m1_weekly_all = None
        self.m1_daily_all = None
        self.m1_weekly_cow = None
        self.m1_daily_cow = None
        self.default_wy_id = None
        self.days_on_date_of_change = None

        self.m1_maxdiff_weekly_avg = None
        self.m1_maxdiff_daily_avg = None
        self.avg_zscore_per_row_weekly_log = None
        self.avg_zscore_per_row_weekly_log_index = None
        self.avg_zscore_per_row_daily_log = None
        self.avg_zscore_per_row_daily_log_index = None

        self.output_folder = r"F:\COWS\data\milk_data\lactations\plots"


    def load_and_process(self, wy_id=None):
   
        LLS = get_dependency('lactations_log_standard')

        self.m1_weekly_all = LLS.m1_weekly_all
        self.m1_daily_all  = LLS.m1_daily_all
        doc = LLS.days_on_date_of_change #to be used for a vert line in the plot

        # Set default WY_id if not provided
        if wy_id is None and self.m1_weekly_all is not None:
            self.default_wy_id = self.m1_weekly_all.columns[0]
        else:
            self.default_wy_id = wy_id

        # Now get cow-specific data using the correct WY_id
        self.m1_weekly_cow = LLS.m1_weekly_all.loc[:, self.default_wy_id] if self.default_wy_id in LLS.m1_weekly_all.columns else None
        self.m1_daily_cow  = LLS.m1_daily_all.loc[:, self.default_wy_id] if self.default_wy_id in LLS.m1_daily_all.columns else None

        # Set days_on_date_of_change only if > 0 for this cow
        self.days_on_date_of_change = None
        if doc is not None and hasattr(doc, 'columns') and self.default_wy_id in doc.columns:
            value = doc[self.default_wy_id].values[0]
            if value > 0:
                self.days_on_date_of_change = value        

        # Assign maxdiff averages for plotting
        self.m1_maxdiff_weekly_avg = LLS.maxdiff_weekly_avg_all if hasattr(LLS, "maxdiff_weekly_avg_all") else None
        self.m1_maxdiff_daily_avg  = LLS.m1_maxdiff_daily_avg_all if hasattr(LLS, "m1_maxdiff_daily_avg_all") else None

        # Compute z-score logs for weekly and daily using numpy
        if self.m1_weekly_all is not None:
            weekly_log = np.log(self.m1_weekly_all.values)
            avg_zscore_weekly = (weekly_log - weekly_log.mean(axis=1, keepdims=True)).mean(axis=1)
            self.avg_zscore_per_row_weekly_log = avg_zscore_weekly
            self.avg_zscore_per_row_weekly_log_index = self.m1_weekly_all.index

        if self.m1_daily_all is not None:
            daily_log = np.log(self.m1_daily_all.values)
            avg_zscore_daily = (daily_log - daily_log.mean(axis=1, keepdims=True)).mean(axis=1)
            self.avg_zscore_per_row_daily_log = avg_zscore_daily
            self.avg_zscore_per_row_daily_log_index = self.m1_daily_all.index

        self.plot_all_v_cow(wy_id=self.default_wy_id)
        self.plot_maxdiff()
        # self.plot_zscore_comparison()


    def plot_all_v_cow(self, wy_id=None):
        fig, ax1 = plt.subplots(figsize=(14, 7))
        weekly_avg_all = self.m1_weekly_all.mean(axis=1)
        ax1.plot(np.arange(0, 52), weekly_avg_all.head(52).values, 'o-', color='blue', label='Weekly Liters - all cows')
        ax1.set_xlabel('Weekly')
        ax1.set_ylabel('Liters (Raw)', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)
        ax1.grid(axis='x', linestyle=':', alpha=0.7)
        ax1.set_xlim(1, 52)
        ax1.set_xticks(np.arange(0, 52, 5))
        if self.days_on_date_of_change is not None:
            ax1.axvline(self.days_on_date_of_change, color='red', linestyle='--', linewidth=1, label='Date of Change')


        ax2 = ax1.twiny()
        weekly_cow = self.m1_weekly_cow
        daily_cow  = self.m1_daily_cow
        ax2.plot(
            np.arange(1, 53),
            weekly_cow.head(52).values,
            'o-',
            color='red',
            label=f'Cow Liters (WY_id {wy_id})',
            alpha=0.7
        )
        if daily_cow is not None:
            ax2.plot(
                np.arange(1, len(daily_cow) + 1),
                daily_cow.values,
                linestyle=':',
                linewidth=2,
                color='pink',
                label=f'Daily Liters (WY_id {wy_id})',
                alpha=0.5
            )   
        ax2.set_xlabel('week')
        ax2.set_xlim(1, 52)
        ax2.grid(axis='x', linestyle=':', alpha=0.7)


         # Add a duplicate x-axis for daily (top), with divisions of 7 days starting at 0
        ax3 = ax1.twiny()
        ax3.spines['top'].set_position(('outward', 40))
        num_days = len(daily_cow) if daily_cow is not None else 364
        ax3.set_xlim(0, num_days)
        day_ticks = np.arange(0, num_days + 1, 7)
        ax3.set_xticks(day_ticks)
        ax3.set_xlabel('Day')
        # Very light vertical grid for days
        for tick in day_ticks:
            ax3.axvline(x=tick, color='gray', linestyle=':', alpha=0.15, zorder=0)


        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.title(f"WY_id {wy_id} : Liters weekly" if wy_id else 'Liters: all_v_cow', fontsize=20 )
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')
        # print(f"YYYYYYYY base64string (first 20 chars): {img_base64[:20]}")
        return img_base64

    def plot_maxdiff(self):
        fig, ax1 = plt.subplots(figsize=(14, 7))
        if self.m1_maxdiff_weekly_avg is not None:
            ax1.plot(np.arange(1, 53), self.m1_maxdiff_weekly_avg.head(52).values, 'o-', color='blue', label='Weekly Avg MaxDiff')
        ax1.set_xlabel('Week')
        ax1.set_ylabel('Avg MaxDiff (from max)')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)
        ax1.grid(axis='x', linestyle=':', alpha=0.7)
        ax1.set_xlim(1, 52)

        ax2 = ax1.twiny()
        if self.m1_maxdiff_daily_avg is not None:
            ax2.plot(np.arange(1, 365), 
                    self.m1_maxdiff_daily_avg.head(364).values, 
                    linestyle='dotted',
                    linewidth=2.5, 
                    color='orange', 
                    label='Daily Avg MaxDiff', 
                    alpha=0.7)
        ax2.set_xlabel('Day')
        ax2.set_xlim(1, 364)
        ax2.grid(axis='x', linestyle=':', alpha=0.7)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

        plt.title('Weekly / Daily MaxDiff avg')
        plt.tight_layout()
        output_path = f"{self.output_folder}\\maxdiff_avg.png"
        plt.savefig(output_path)
        plt.close(fig)
        print(f"maxdiff_avg.png updated at {output_path}")

    def plot_zscore_comparison(self):
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax1.plot(self.avg_zscore_per_row_weekly_log_index, self.avg_zscore_per_row_weekly_log, 'o-', color='blue', label='Weekly Avg Z-Score')
        ax1.set_xlabel('Week')
        ax1.set_ylabel('Weekly Avg Z-Score', color='blue')
        ax1.tick_params(axis='y', labelcolor='blue')
        ax1.grid(axis='y', linestyle='--', alpha=0.7)

        ax2 = ax1.twiny()
        ax2.plot(self.avg_zscore_per_row_daily_log_index, self.avg_zscore_per_row_daily_log, '--', color='red', label='Daily Avg Z-Score')
        ax2.set_xlabel('Day')
        ax2.set_ylabel('Daily Avg Z-Score', color='red')
        ax2.tick_params(axis='y', labelcolor='red')

        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        plt.title('Comparison of Weekly and Daily Avg Z-Score')
        plt.tight_layout()
        # output_path = f"{self.output_folder}\\zscore_comparison.png"
        # plt.savefig(output_path)
        # plt.close(fig)
        # print(f"zscore_comparison.png updated at {output_path}")


        
if __name__ == "__main__":
    plots = LactationPlots()
    plots.load_and_process()
