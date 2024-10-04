import importlib
import json
import pandas as pd
from dash_app.module_config import CustomJSONEncoder

mc = CustomJSONEncoder()


def load_class_and_create_json(module_name, get_dash_vars_method):
    # Load the module
    module = importlib.import_module(module_name)
    
    # Get the data from the method
    data = get_dash_vars_method()
    
    # Convert the data to JSON
    json_data = json.dumps(data, cls=CustomJSONEncoder)
    return json_data

def load_json_and_create_instance(json_data, module_name, class_name):
    # Deserialize the JSON data
    data = mc.deserialize_module_data(json_data)
    
    # Load the module and class
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    
    # Create an instance of the class
    instance = class_()
    
    # Populate the instance with the data
    for attr, value in data.items():
        if isinstance(value, dict) and 'data' in value and 'index' in value:
            # Convert back to DataFrame
            df = pd.DataFrame(value['data'])
            df.index = pd.Index(value['index'])
            setattr(instance, attr, df)
        else:
            setattr(instance, attr, value)
    
    return instance

def instance_to_dict(instance):
    data = {}
    for attr, value in instance.__dict__.items():
        if isinstance(value, pd.DataFrame):
            data[attr] = {
                'data': value.to_dict(orient='records'),
                'columns' : value.columns.tolist(),
                'index': value.index.tolist()
            }
        elif isinstance(value, pd.Series):
            data[attr] = value.to_list()
        else:
            data[attr] = value
    return data