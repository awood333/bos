import json
import pandas as pd
import importlib
from dash_app.module_config import serialize_module_data, deserialize_module_data, CustomJSONEncoder

def load_class_and_create_json(module_name, get_dash_vars_method):
    # Load the module
    module = importlib.import_module(module_name)
    
    data = get_dash_vars_method()
    
    # Convert the data to JSON
    json_data = json.dumps(data, cls=CustomJSONEncoder)
    return json_data

def load_json_and_create_instance(json_data, module_name, class_name):
    data = deserialize_module_data(json_data)
    module = __import__(module_name, fromlist=[class_name])
    class_ = getattr(module, class_name)
    instance = class_(**data)
    instance_from_dict(instance, data)
    return instance

def instance_to_dict(instance):
    data = {}
    for attr, value in instance.__dict__.items():
        if isinstance(value, pd.DataFrame):
            data[attr] = value.to_dict()
        else:
            data[attr] = value
    return data

def instance_from_dict(instance, data):
    for attr, value in data.items():
        if isinstance(value, dict):
            try:
                instance.__dict__[attr] = pd.DataFrame.from_dict(value)
            except ValueError:
                instance.__dict__[attr] = value
        else:
            instance.__dict__[attr] = value