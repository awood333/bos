import json
import pandas as pd
import io
from dash import dcc, html, dash_table, Dash
from dash.dependencies import Input, Output
from dash_app.dynamic_loader import load_class_and_create_json
from dash_app.module_config import modules
from dash_app.formatting import DataFrameFormatter

app = Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(id='module-dropdown', options=[{'label': k, 'value': k} for k in modules.keys()]),
    dcc.Dropdown(id='table-dropdown'),
    html.Div(id='table-container')
])

# Callback to update the table dropdown based on the selected module
@app.callback(
    Output('table-dropdown', 'options'),
    Output('table-dropdown', 'value'),
    Input('module-dropdown', 'value')
)
def update_table_dropdown(selected_module):
    if not selected_module:
        return [], None

    module_info = modules[selected_module]
    json_data = load_class_and_create_json(module_info['module_name'], module_info['get_dash_vars'])
    
    # Deserialize the JSON string to a Python dictionary
    data_dict = json.loads(json_data)
    
    # Access the keys from the deserialized dictionary
    options = [{'label': name, 'value': name} for name in data_dict.keys()]
    value = list(data_dict.keys())[0] if options else None
    return options, value

# Callback to update the table based on the selected table
@app.callback(
    Output('table-container', 'children'),
    Input('module-dropdown', 'value'),
    Input('table-dropdown', 'value')
)
def update_table(selected_module, selected_table):
    if not selected_table:
        return html.Div()
    
    module_info = modules[selected_module]
    json_data = load_class_and_create_json(module_info['module_name'], module_info['get_dash_vars'])
    
    # Deserialize the JSON string to a Python dictionary
    data_dict = json.loads(json_data)
    
    # Convert the selected table's JSON data back to a DataFrame
    df = pd.DataFrame.from_dict(data_dict[selected_table])
    df.reset_index(inplace=True)
    
    # Use the DataFrameFormatter class
    df_formatter = DataFrameFormatter(df)
    formatted_df = df_formatter.format_dataframe()
    
    return dash_table.DataTable(
        columns=[{"name": i, "id": i} fo.r i in formatted_df.columns],
        data=formatted_df.to_dict('records')
    )

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)