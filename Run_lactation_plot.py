'''
Run_lactation_plot
'''
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from container import get_dependency
import pandas as pd
import numpy as np

output_folder1 = r"Q:\My Drive\COWS\data\plots\lactation_plots"
output_folder2 = r"F:\COWS\data\milk_data\lactations\plots\weekly\latest_plots"
os.makedirs(output_folder1, exist_ok=True)

TL = get_dependency('this_lactation')
LLS = get_dependency('lactations_log_standard')
doc = LLS.days_on_date_of_change
cow_idx = 94
value_test = doc.iloc[doc.index.get_loc(cow_idx), 0]

df_weekly = TL.milking_wkly.iloc[:53, :].copy().dropna(axis=1, how='all')
df_daily  = TL.milking_daily.iloc[:364, :].copy().dropna(axis=1, how='all')
df_id_list = df_weekly.columns.to_frame(index=False, name="WY_id")
df_id_list.to_csv(os.path.join(output_folder1, "cow_id_list.csv"), index=False)
df_id_list.to_csv(os.path.join(output_folder2, "cow_id_list.csv"), index=False)

# Compute the weekly average for all cows
weekly_avg_all = df_weekly.mean(axis=1)
daily_avg_all  = df_daily.mean(axis=1)

for cow_id in df_weekly.columns:

        # Get days_on_date_of_change for this cow_id
    days_on_date_of_change = None
    if doc is not None:
        try:
            cow_id_int = int(cow_id)
            value = doc.iloc[doc.index.get_loc(cow_id_int), 0]
            if not pd.isna(value) and value > 0:
                days_on_date_of_change = value
        except Exception as e:
            print(f"Error for cow_id {cow_id}: {e}")
    # print('days_on_date_of_change: ', days_on_date_of_change)


    fig, ax1 = plt.subplots(figsize=(14, 7))
    # Plot the average weekly line (blue)
    ax1.plot(df_weekly.index, weekly_avg_all.values, '-', color='blue', label='weekly avg liters/day - all cows', alpha=.8)
    # Plot the individual cow weekly (red)
    ax1.plot(df_weekly.index, df_weekly[cow_id].values, 'o-', color='red', label=f'weekly avg liters/day - WY#{cow_id}')
    ax1.set_xlabel('Weeks Milking')
    ax1.set_ylabel('Liters', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.grid(axis='x', linestyle=':', alpha=0.7)
    ax1.set_xlim(df_weekly.index.min(), df_weekly.index.max())

# Here are some common linestyle options:

# '-' : solid line
# '--': dashed line
# '-.': dash-dot line
# ':' : dotted line




    # Plot daily data for this cow if available
    if cow_id in df_daily.columns:
        ax2 = ax1.twiny()
        ax2.plot(df_daily.index, df_daily[cow_id].values, 
                 linestyle='-', 
                 linewidth=1, 
                 marker='o',
                 markersize=1,
                 color='orange', 
                 label=f'Cow WY_id {cow_id} (daily avg liters)', 
                 alpha=0.7)
        ax2.set_xlabel('Days Milking')
        ax2.set_xlim(df_daily.index.min(), df_daily.index.max())
        day_ticks = np.arange(df_daily.index.min(), df_daily.index.max() + 1, 7)
        ax2.set_xticks(day_ticks)
        ax2.set_xticklabels(day_ticks, rotation=90)
        ax2.grid(axis='x', linestyle=':', alpha=0.3)

        # Draw vertical gridline if days_on_date_of_change is valid
        if days_on_date_of_change is not None:
            ax2.axvline(days_on_date_of_change, color='green', linestyle='--', linewidth=3, label='Date of Change')


        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    else:
        ax1.legend(loc='upper right')

    plt.title(f'WY {cow_id}', fontsize=20)
    plt.tight_layout()
    output_path1 = os.path.join(output_folder1, f"cow_{cow_id}_weekly_daily.png")
    output_path2 = os.path.join(output_folder2, f"cow_{cow_id}_weekly_daily.png")
    plt.savefig(output_path1)
    plt.savefig(output_path2)

    plt.close()
