# json_utils.py
import json
import pandas as pd
from datetime import datetime

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)

def custom_json_decoder(dct):
    for key, value in dct.items():
        try:
            dct[key] = pd.to_datetime(value)
        except (ValueError, TypeError):
            pass
    return dct