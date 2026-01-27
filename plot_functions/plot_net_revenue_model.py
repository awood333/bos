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
        self.date_of_change1 = None
        self.date_of_change2 = None
        self.output_folder = None

    def load_and_process(self):
        wdg = get_dependency('wet_dry_groups')
        self.df = wdg.net_revenue_wet_dry_df.copy()
        LLS = get_dependency('lactations_log_standard')
        self.date_of_change1 = pd.to_datetime(getattr(LLS, 'date_of_change1', None))
        self.date_of_change2 = pd.to_datetime(getattr(LLS, 'date_of_change2', None))
        self.df['date'] = pd.to_datetime(self.df['date'])

        self.output_folder = r"Q:\My Drive\COWS\data\plots\net_revenue_plots"
        os.makedirs(self.output_folder, exist_ok=True)

        for cow_id in self.df['WY_id'].unique():
            cow_df = self.df[self.df['WY_id'] == cow_id].sort_values('date')
            # Group by week
            weekly = cow_df.groupby('week').agg({
                'revenue': 'mean',
                'feedcost': 'mean',
                'net_revenue': 'mean',
                'date': 'first'
            }).reset_index()
            if weekly[['revenue', 'feedcost', 'net_revenue']].isnull().all().all():
                continue

            fig, ax1 = plt.subplots(figsize=(14, 8))
            # Stacked bar for revenue and feedcost
            ax1.bar(weekly['date'], weekly['feedcost'], color='#f7cbe0', label='Feedcost', width=10)
            ax1.bar(weekly['date'], weekly['revenue'], color='#cef0d1', label='Revenue', width=10, bottom=weekly['feedcost'])
            ax1.set_ylabel('Weekly Revenue / Feedcost', color='black')
            ax1.tick_params(axis='y', labelcolor='black')
            # Secondary axis for net_revenue
            ax2 = ax1.twinx()
            ax2.plot(weekly['date'], weekly['net_revenue'], 'o-', color='purple', label='Net Revenue')
            ax2.set_ylabel('Weekly Net Revenue', color='purple')
            ax2.tick_params(axis='y', labelcolor='purple')
            # Date ticks
            week_ticks = pd.date_range(self.df['date'].min(), self.df['date'].max(), freq='14D')
            ax1.set_xticks(week_ticks)
            ax1.set_xticklabels([dt.strftime('%m/%d') for dt in week_ticks], rotation=90)
            for tick in week_ticks:
                ax1.axvline(tick, color='gray', linestyle=':', alpha=0.3, zorder=0)
            # Day of change vertical lines
            if pd.notna(self.date_of_change1):
                ax1.axvline(self.date_of_change1, color='green', linestyle='--', linewidth=2, label='Day of Change 1')
            if pd.notna(self.date_of_change2):
                ax1.axvline(self.date_of_change2, color='red', linestyle='--', linewidth=2, label='Day of Change 2')
            ax1.grid(axis='y', linestyle='--', alpha=0.7)
            ax1.set_xlim(self.df['date'].min(), self.df['date'].max())
            # Legends
            handles1, labels1 = ax1.get_legend_handles_labels()
            handles2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(handles1 + handles2, labels1 + labels2, loc='upper center', facecolor='lightgreen', framealpha=0.25)
            plt.title(f'Net Revenue - WY {cow_id}', fontsize=20)
            plt.tight_layout()
            output_path = os.path.join(self.output_folder, f"cow_{cow_id}_net_revenue.png")
            plt.savefig(output_path)
            plt.close()

if __name__ == "__main__":
    obj=PlotNetRevenueModel()
    obj.load_and_process()
    