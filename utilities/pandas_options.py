# utilities/pandas_options.py
import pandas as pd

class PandasOptions:
    def __init__(self):
        self.options = {
            # Display Options
            'display.max_rows': 1000,
            'display.max_columns': 30,
            'display.width': 1000,
            'display.max_colwidth': 100,
            'display.precision': 2,
            'display.float_format': '{:.2f}'.format,
            'display.colheader_justify': 'right',
            'display.expand_frame_repr': False,

            # Mode Options
            'mode.chained_assignment': 'warn',
            'mode.sim_interactive': True,

            # IO Options
            'io.excel.xlsx.writer': 'openpyxl',
            'io.hdf.default_format': 'table'
        }

    def set_options(self, options=None):
        if options is None:
            options = self.options
        for option, value in options.items():
            pd.set_option(option, value)

# Automatically set pandas options when this module is imported
# pandas_options = PandasOptions()
# pandas_options.set_options()