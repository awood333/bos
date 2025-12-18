'''milk_functions.lactation.lactation_measurements.lactation_dashboard'''
import io
import base64
import threading
import webbrowser

import dash
from dash import dcc, html, Input, Output, State, dash_table

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from milk_functions.lactation.lactation_measurements.lactations_log_standard import LactationsLogStandard
from milk_functions.lactation.lactation_measurements.lactation_plots import LactationPlots
from insem_functions.I_U_merge import I_U_merge

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Lactation Dashboard"),
    html.Div([
        dcc.Input(
            id='wyid-input',
            type='number',
            value="",
            min=1,
            step=1,
            style={'marginRight': '10px'}
        ),
        html.Button('Submit', id='submit-btn', n_clicks=0)
    ], style={'marginBottom': '20px'}),
    html.Img(
        id='lact-plot',
        style={'width': '100%', 'height': 'auto', 'border': '1px solid #ccc', 'marginBottom': '20px'}
    ),
    dash_table.DataTable(
        id='iu-table',
        style_table={
            'overflowY': 'auto',
            'maxHeight': '500px',
            'overflowX': 'auto'
        },
        style_cell_conditional=[
            {'if': {'column_id': 'datex'}, 'width': '160px'},
            {'if': {'column_id': 'typex'}, 'width': '140px'},
        ]
    )
])

def get_lactation_plot(wy_id):
    lact = LactationsLogStandard()
    lact.WY_id = wy_id
    lact.load_and_process()
    plots = LactationPlots()
    plots.load_and_process(lact)
    img_base64 = plots.plot_all_v_cow(wy_id=wy_id)
    return f"data:image/png;base64,{img_base64}"

def get_iu_table(wy_id):
    iu_merge = I_U_merge()
    iu_merge.load_and_process()
    df = iu_merge.iu
    # Filter for the selected WY_id
    df_filtered = df[df['WY_id'] == wy_id].copy()
    # Ensure 'datex' is string (YYYY-MM-DD)
    if 'datex' in df_filtered.columns:
        df_filtered['datex'] = df_filtered['datex'].astype(str)
    # Show only first 20 rows for display
    return df_filtered.head(20)

@app.callback(
    Output('lact-plot', 'src'),
    Output('iu-table', 'data'),
    Output('iu-table', 'columns'),
    Input('submit-btn', 'n_clicks'),
    State('wyid-input', 'value')
)
def update_dashboard(n_clicks, wy_id):
    if not wy_id:
        return dash.no_update, dash.no_update, dash.no_update
    img_src = get_lactation_plot(wy_id)
    df = get_iu_table(wy_id)
    columns = [{"name": i, "id": i} for i in df.columns]
    data = df.to_dict('records')
    return img_src, data, columns


if __name__ == '__main__':
    def open_browser():
        try:
            webbrowser.get('firefox').open_new("http://127.0.0.1:8052/")
        except Exception as e:
            print(f"Could not open Firefox: {e}")
            try:
                webbrowser.open("http://127.0.0.1:8052/")
            except Exception as e2:
                print(f"Could not open default browser: {e2}")

    threading.Timer(1.5, open_browser).start()
    app.run_server(debug=False, port=8052)