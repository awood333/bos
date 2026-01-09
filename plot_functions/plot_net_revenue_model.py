'''
plot_functions.plot_net_revenue_model
'''
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from container import get_dependency


class PlotNetRevenueModel:
    def __init__(self):
        self.df = None
        self.date_of_change = None
        self.output_folder = None

    def load_and_process(self):
        wdg = get_dependency('wet_dry_groups')
        self.df = wdg.net_revenue_wet_dry_df.copy()
        LLS = get_dependency('lactations_log_standard')
        self.date_of_change = pd.to_datetime(getattr(LLS, 'date_of_change', None))
        self.df['date'] = pd.to_datetime(self.df['date'])

        self.output_folder = r"Q:\My Drive\COWS\data\plots\net_revenue_plots"
        os.makedirs(self.output_folder, exist_ok=True)

        for cow_id in self.df['WY_id'].unique():
            cow_df = self.df[self.df['WY_id'] == cow_id].sort_values('date')
            y_weekly = cow_df.groupby('week')['net_revenue'].mean()
            if y_weekly.isnull().all():
                continue

            fig, ax = plt.subplots(figsize=(14, 7))
            ax.plot(y_weekly.index, y_weekly.values, 'o-', color='purple', label=f'Weekly Net Revenue WY#{cow_id}')
            if pd.notna(self.date_of_change):
                ax.axvline(self.date_of_change, color='green', linestyle='--', linewidth=2, label='Day of Change')
            ax.set_xlabel('Date (mm/dd)')
            ax.set_ylabel('Weekly Net Revenue ', color='purple')
            ax.tick_params(axis='y', labelcolor='purple')
            week_ticks = pd.date_range(self.df['date'].min(), self.df['date'].max(), freq='14D')
            ax.set_xticks(week_ticks)
            ax.set_xticklabels([dt.strftime('%m/%d') for dt in week_ticks], rotation=90)
            for tick in week_ticks:
                ax.axvline(tick, color='gray', linestyle=':', alpha=0.3, zorder=0)
            ax.grid(axis='y', linestyle='--', alpha=0.7)
            ax.set_xlim(self.df['date'].min(), self.df['date'].max())
            ax.legend(loc='upper center', facecolor='lightgreen', framealpha=0.25)
            plt.title(f'Net Revenue - WY {cow_id}', fontsize=20)
            plt.tight_layout()
            output_path = os.path.join(self.output_folder, f"cow_{cow_id}_net_revenue.png")
            plt.savefig(output_path)
            plt.close()

if __name__ == "__main__":
    obj=PlotNetRevenueModel()
    obj.load_and_process()
    