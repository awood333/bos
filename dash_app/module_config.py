import json
import pandas as pd
import numpy as np
import logging


from insem_functions.Insem_ultra_data   import InsemUltraData
from insem_functions.insem_ultra_basics import InsemUltraBasics
from finance_functions.NetRevenue       import MilkNetRevenue

from finance_functions.BKK_bank         import BKK_bank
from feed_functions.feed_cost_basics    import FeedCostBasics
from milk_functions.status_data          import StatusData


IUD = InsemUltraData()
IUD_vars = IUD.get_dash_vars

IUB = InsemUltraBasics()
IUB_vars = IUB.get_dash_vars


MI = MilkNetRevenue()
MI_vars = MI.get_dash_vars

BKK = BKK_bank()
BKK_vars = BKK.get_dash_vars

FCB = FeedCostBasics()
FCB_vars = FCB.get_dash_vars

SD = StatusData()
SD_vars = SD.get_dash_vars



logging.basicConfig(level=logging.DEBUG)

modules = {
    'InsemUltraData': {
        'module_name': 'insem_functions.InsemUltraData',
        'get_dash_vars': IUD_vars
    }
    ,
        'InsemUltraBasics': {
        'module_name': 'insem_functions.InsemUltraBasics',
        'get_dash_vars': IUB_vars
    }
    ,
    
    'MilkNetRevenue': {
        'module_name': 'finance.NetRevenue',
        'get_dash_vars': MI_vars,
        },
    
    'BKK_bank': {
        'module_name': 'finance.BKK_bank',
        'get_dash_vars': BKK_vars,
        },
    
    'Feed Cost': {
        'module_name': 'feed_related.feed_cost_basics',
        'get_dash_vars': FCB_vars,
        },
        
    'Status IDs': {
        'module_name': 'milk_functions.status_ids',
        'get_dash_vars': SD_vars,
        },
            
    # 'days of milking': {
    #     'module_name': 'milk_functions.days_of_milking',
    #     'get_dash_vars': SD_vars,
    #     },
    
    # 'play' : {
    #     'module_name' : 'play',
    #     'get_dash_vars': play_vars,
    # }
    
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
