import os
import matplotlib.pyplot as plt
from container import get_dependency
import numpy as np

output_folder = r"Q:\My Drive\COWS\lactation_plots"
os.makedirs(output_folder, exist_ok=True)

TL = get_dependency('this_lactation')
df_weekly = TL.milking_wkly.iloc[:53, :].copy().dropna(axis=1, how='all')
df_daily  = TL.milking_daily.iloc[:364, :].copy().dropna(axis=1, how='all')
df_id_list = df_weekly.columns.to_frame(index=False, name="WY_id")
df_id_list.to_csv(os.path.join(output_folder, "cow_id_list.csv"), index=False)

# Compute the weekly average for all cows
weekly_avg_all = df_weekly.mean(axis=1)
daily_avg_all  = df_daily.mean(axis=1)

for cow_id in df_weekly.columns:
    fig, ax1 = plt.subplots(figsize=(14, 7))
    # Plot the average weekly line (blue)
    ax1.plot(df_weekly.index, weekly_avg_all.values, 'o-', color='blue', label='Weekly Mean Liters - all cows')
    # Plot the individual cow weekly (red)
    ax1.plot(df_weekly.index, df_weekly[cow_id].values, 'o-', color='red', label=f'Cow WY_id {cow_id} (weekly)')
    ax1.set_xlabel('Week')
    ax1.set_ylabel('Liters (Weekly)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.grid(axis='x', linestyle=':', alpha=0.7)
    ax1.set_xlim(df_weekly.index.min(), df_weekly.index.max())

    # Plot daily data for this cow if available
    if cow_id in df_daily.columns:
        ax2 = ax1.twiny()
        ax2.plot(df_daily.index, df_daily[cow_id].values, linestyle=':', linewidth=2, color='orange', label=f'Cow WY_id {cow_id} (daily)', alpha=0.7)
        ax2.set_xlabel('Day')
        ax2.set_xlim(df_daily.index.min(), df_daily.index.max())
        day_ticks = np.arange(df_daily.index.min(), df_daily.index.max() + 1, 7)
        ax2.set_xticks(day_ticks)
        ax2.set_xticklabels(day_ticks, rotation=90)
        ax2.grid(axis='x', linestyle=':', alpha=0.3)

        # Add a light vertical grid for days every 7 days
        for tick in day_ticks:
            ax2.axvline(x=tick, color='gray', linestyle=':', alpha=0.1, zorder=0)

        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    else:
        ax1.legend(loc='upper right')

    plt.title(f'Weekly & Daily Liters for WY_id {cow_id}')
    plt.tight_layout()
    output_path = os.path.join(output_folder, f"cow_{cow_id}_weekly_daily.png")
    plt.savefig(output_path)
    plt.close()
    print(f"Saved: {output_path}")