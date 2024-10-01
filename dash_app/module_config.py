import json
import pandas as pd
import numpy as np
import logging

from insem_functions.InsemUltraFunctions import InsemUltraFunctions
from insem_functions.InsemUltraBasics import InsemUltraBasics
from insem_functions.InsemUltraData import InsemUltraData
IUF = InsemUltraFunctions()
IUF_vars  = IUF.get_dash_vars

IUD = InsemUltraData()
IUD_vars = IUD.get_dash_vars

IUB = InsemUltraBasics()
IUB_vars = IUB.get_dash_vars

logging.basicConfig(level=logging.DEBUG)

modules = {
    'InsemUltraData': {
        'module_name': 'insem_functions.InsemUltraData',
        'get_dash_vars': IUD_vars,
    },
    'InsemUltraFunctions': {
        'module_name': 'insem_functions.InsemUltraFunctions',
        'get_dash_vars': IUF_vars,
    },
    'InsemUltraBasics': {
        'module_name': 'insem_functions.InsemUltraBasics',
        'get_dash_vars': IUB_vars,
    },
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

# Create instances of each class
# iuf = InsemUltraFunctions()
# iub = InsemUltraBasics()
# iud = InsemUltraData()

# # Get the dash vars from each instance
# iuf_dash_vars = iuf.get_dash_vars()
# iub_dash_vars = iub.get_dash_vars()
# iud_dash_vars = iud.get_dash_vars()

# # Serialize the data
# json_data_iuf = serialize_module_data(iuf_dash_vars)
# json_data_iub = serialize_module_data(iub_dash_vars)
# json_data_iud = serialize_module_data(iud_dash_vars)

# # Log the serialized data
# logging.debug(f"Serialized data from InsemUltraFunctions: {json_data_iuf[:10]}")
# logging.debug(f"Serialized data from InsemUltraBasics: {json_data_iub[:10]}")
# logging.debug(f"Serialized data from InsemUltraData: {json_data_iud[:10]}")