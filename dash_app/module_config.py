import json
import pandas as pd
import numpy as np
import logging


from insem_functions.InsemUltraData import InsemUltraData
from finance.milk_net_revenue       import MilkNetRevenue

IUD = InsemUltraData()
IUD_vars = IUD.get_dash_vars

MI = MilkNetRevenue()
MI_vars = MI.get_dash_vars

logging.basicConfig(level=logging.DEBUG)

modules = {
    'InsemUltraData': {
        'module_name': 'insem_functions.InsemUltraData',
        'get_dash_vars': IUD_vars
    },
    'MilkNetRevenue': {
        'module_name': 'finance.milk_net_revenue',
        'get_dash_vars': MI_vars,
    }
}

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if obj is pd.NaT:
            return None
        if isinstance(obj, np.datetime64):
            return str(obj)
        if isinstance(obj, pd.Series):
            return obj.to_list()
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')  # Convert DataFrame to list of dictionaries
        return super().default(obj)

def serialize_module_data(data):
    logging.debug(f"Data type being serialized: {type(data)}")
    return json.dumps(data  , cls=CustomJSONEncoder)

def deserialize_module_data(json_data):
    return json.loads(json_data)
