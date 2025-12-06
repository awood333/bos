import pandas as pd
from container import get_dependency

class CompareModelWhiteboardGroups_Last:

    '''compares the last day groups for the dash_report_milk.py'''
    
    def __init__(self):
        self.model_groups = None
        self.whiteboard_groups = None
        self.comparison = None

    def load_and_process(self):
        # Get groups from both sources
        self.model_groups = get_dependency('model_groups')
        self.whiteboard_groups = get_dependency('whiteboard_groups')
        self.comparison = self.compare_groups()

    def compare_groups(self):
        # Get last row of model groups
        model = self.model_groups.model_groups_lastrow.copy()
        wb = self.whiteboard_groups.whiteboard_groups_tenday.copy()

        # Ensure WY_id is int for both
        model['WY_id'] = model['WY_id'].astype(int)
        wb['WY_id'] = wb['WY_id'].astype(int)

        # Rename columns for clarity
        model = model.rename(columns={'group': 'model group'})
        wb = wb.rename(columns={'group': 'whiteboard group'})

        # Keep only 'WY_id' and 'whiteboard group' columns in wb
        wb = wb[['WY_id', 'whiteboard group']]

        # Merge on WY_id
        merged = pd.merge(model, wb, on="WY_id", suffixes=('_model', '_whiteboard'))

        # Reorder columns so 'model group' is last
        cols = merged.columns.tolist()
        if 'model group' in cols:
            cols.remove('model group')
            cols.append('model group')
        merged = merged[cols]

        # Add comparison column
        merged['comp'] = merged['whiteboard group'] == merged['model group']
        merged['comp'] = merged['comp'].map({True: '', False: 'X'})

        # Optional: sort by avg if present
        if 'avg' in merged.columns:
            merged = merged.sort_values(by='avg', ascending=False)

        return merged

    def write_to_csv(self, path="F:\\COWS\\data\\milk_data\\groups\\compare_model_whiteboard_groups.csv"):
        if self.comparison is not None:
            self.comparison.to_csv(path, index=False)

if __name__ == "__main__":
    cmp = CompareModelWhiteboardGroups_Last()
    cmp.load_and_process()
    cmp.write_to_csv()