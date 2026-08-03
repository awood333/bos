'''LactationPlots.py'''
import inspect
import matplotlib.pyplot as plt
from milk_functions.lactation.weekly_lactations import WeeklyLactations


class LactationPlots:
    def __init__(self):
        print(f"LactationPlots instantiated by: {inspect.stack()[1].filename}")
        self.WL = None
        self.live_weekly_lactations = None
        self.fig = None
        self.ax = None

    def load(self):
        self.WL = WeeklyLactations()
        self.WL.load()
        self.process()

    def process(self):
        # Use only live cow lactations (already filtered by alive_ids)
        self.live_weekly_lactations = [
            self.WL.live_lact_wk_1,
            self.WL.live_lact_wk_2,
            self.WL.live_lact_wk_3,
            self.WL.live_lact_wk_4,
            self.WL.live_lact_wk_5,
        ]
        return self.live_weekly_lactations

    def plot(self):
        fig, ax = plt.subplots(figsize=(12, 6))

        labels = [
            'Lactation 1',
            'Lactation 2',
            'Lactation 3',
            'Lactation 4',
            'Lactation 5',
        ]
        colors = ['C0', 'C1', 'C2', 'C3', 'C4']

        for lact_df, label, color in zip(self.live_weekly_lactations, labels, colors):
            weekly_avg = lact_df.mean(axis=1)
            weeks = weekly_avg.index + 1  # weeks 1 to 44
            ax.plot(
                weeks,
                weekly_avg,
                marker='o',
                markersize=4,
                linewidth=2,
                label=label,
                color=color
            )

        ax.set_xlim(1, 44)
        ax.set_xticks(range(1, 45, 4))
        ax.set_xlabel('Week of Lactation')
        ax.set_ylabel('Average Daily Milk Yield (kg)')
        ax.set_title('Weekly Lactation Curves by Parity (Live Cows Only)')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend()

        self.fig = fig
        self.ax = ax
        plt.tight_layout()
        plt.show()

        return fig, ax


if __name__ == "__main__":
    obj = LactationPlots()
    obj.load()
    obj.plot()