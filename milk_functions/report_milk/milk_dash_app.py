'''milk_funtions.report_milk.milk_dash_app.py'''

import threading
import webbrowser

from dash import Dash, html, dash_table
import pandas as pd
from container import get_dependency


COLUMN_WIDTHS = {
    'WY_id': '90px',
    'avg': "80px",
    'AM': '50px',
    'PM': '50px',
    'pct chg from avg': '80px',
    'days milking' : '90px',
    'u_read' : "70px",
    'expected bdate' : "130px",
    'model group' : "60px",
    'whiteboard group' : "60px",
    'comp':"40px",
    # Add more as needed
}
  

def get_panel_style():
    return {
        'border': '3px solid #00bcd4',
        'borderRadius': '15px',
        'padding': '18px',
        'margin': '10px',
        'backgroundColor': '#1D2121',
        'boxShadow': '2px 2px 12px #222',
        'flex': '0 0 auto',
        'minWidth': 'fit-content',
        'maxWidth': 'none',
        'overflowX': 'auto'
    }

def get_table_style():
    return {
        'overflowX': 'auto',
        'maxWidth': '100%',
    }

def get_table_header_style():
    return {
        'backgroundColor': "#1D2121",
        'color': "#a9d0ce",
        'fontWeight': 'bold',
        'textAlign': 'center',
        'fontSize': '22px',
        'height': '60px',
        'verticalAlign': 'middle',
    }

def get_table_cell_style():
    return {
        'fontFamily': 'Courier New, monospace',
        'fontSize': '22px',
        'backgroundColor': "#1D2121",
        'color': "#cdfffd",
        'padding': '2px 4px',
        'whiteSpace': 'normal',
        'minHeight': '30px',
        'border': '1px solid #777878',
    }
    
def get_style_cell_conditional(table_columns):
    """Return style_cell_conditional list for the given columns based on COLUMN_WIDTHS."""
    return [
        {'if': {'column_id': col}, 'width': COLUMN_WIDTHS[col]}
        for col in table_columns if col in COLUMN_WIDTHS
    ]


def get_style_cell_conditional_fortenday(table_columns):
    """Set width for first column as in COLUMN_WIDTHS, next 9 dynamic cols to 120px, rest use COLUMN_WIDTHS if defined."""
    style = []
    # First column (usually WY_id)
    first_col = table_columns[0]
    if first_col in COLUMN_WIDTHS:
        style.append({'if': {'column_id': first_col}, 'width': COLUMN_WIDTHS[first_col]})
    else:
        style.append({'if': {'column_id': first_col}, 'width': '90px'})
    # Next 9 columns (dynamic)
    for col in table_columns[1:11]:
        style.append({'if': {'column_id': col}, 'width': '40px'})
    # Remaining columns, use COLUMN_WIDTHS if available
    for col in table_columns[11:]:
        if col in COLUMN_WIDTHS:
            style.append({'if': {'column_id': col}, 'width': COLUMN_WIDTHS[col]})
    return style
      
def run_milk_dash_app():        
  
    report  = get_dependency('report_milk')         #this calls MA WG MG and CompareGroups
    xl      = get_dependency('report_milk_xlsx')    #ignore pylint: this is enough to run the mod
    plots   = get_dependency('lactation_plots')     #ignore pylint: this is enough to run the mod
    #running 'plots' also calls all the lactation related modules

    tenday_df  = report.tenday
    halfday_df = report.halfday 
    groups_df  =  report.WB_groups
    compare_groups_df = report.CompareGroups



    app = Dash(__name__)

    app.layout = html.Div(
        [
            html.H1(
                f"🐄 Milk Production Report: {pd.Timestamp.now().strftime('%Y-%m-%d')}",
                style={'textAlign': 'center', 'marginBottom': '20px',
                    'fontSize': '32px', 'color': '#333'}
            ),
            html.Div(
                [
                    html.Div([
                        html.H2("10-Day Summary", 
                                style={'textAlign': 'center', 'color': '#00bcd4'}),
                        
                        dash_table.DataTable(
                            id='tenday-table',
                            data=tenday_df.to_dict('records'),
                            columns=[{"name": i, "id": i} for i in tenday_df.columns],
                            style_table=get_table_style(),
                            style_header=get_table_header_style(),
                            style_cell=get_table_cell_style(),
                            # page_size=30,    #this sets max rows
                            cell_selectable=True,
                            style_cell_conditional=get_style_cell_conditional_fortenday(tenday_df.columns),
                            style_data_conditional=[
                                {
                                    'if': {'column_id': 'u_read'},
                                    'textAlign': 'center',
                                    'verticalAlign': 'middle',
                                },
                                
                                {
                                    'if': {
                                        'filter_query': '{pct chg from avg} >= 0.15',
                                        'column_id': 'pct chg from avg'
                                    },
                                    'backgroundColor': "#07f607", 
                                    'color': 'black',
                                    'fontWeight': 'bold'
                                },
                                {
                                    'if': {
                                        'filter_query': '{pct chg from avg} <= -0.15',
                                        'column_id': 'pct chg from avg'
                                    },
                                    'backgroundColor': "#F8E004", 
                                    'color': 'black',
                                    'fontWeight': 'bold'
                                },
                                {
                                    'if': {
                                        'column_id': 'avg'
                                    },
                                    'backgroundColor': "#353B3B", 
                                    'color': '#cdfffd',
                                    'fontWeight': 'bold'
                                },                               
                            ],
                        ),
                    ], style=get_panel_style()),

                    html.Div([
                        html.H2("Half-Day Summary", 
                                style={'textAlign': 'center', 'color': '#00bcd4'}),
                        
                        dash_table.DataTable(
                            id='halfday-table',
                            data=halfday_df.to_dict('records'),
                            columns=[{"name": i, "id": i} for i in halfday_df.columns],
                            style_table=get_table_style(),
                            style_header={**get_table_header_style(), 'height': '100px'},
                            style_cell=get_table_cell_style(),
                            # page_size=30,
                            cell_selectable=True,
                            style_cell_conditional=get_style_cell_conditional(halfday_df.columns),
                        ),
                    ], style=get_panel_style()),


#Groups
                    html.Div([
                        html.H2("Groups", 
                                style={'textAlign': 'center', 'color': '#00bcd4'}),
                        
                        dash_table.DataTable(
                            id='groups-table',
                            data=groups_df.to_dict('records'),
                            columns=[{"name": i, "id": i} for i in groups_df.columns],
                            style_table=get_table_style(),
                            style_header={**get_table_header_style(), 'height': '100px'},
                            style_cell=get_table_cell_style(),
                            cell_selectable=True,
                            style_cell_conditional=get_style_cell_conditional(groups_df.columns),
                            style_data_conditional=[

                                {
                                    'if': {'column_id': 'u_read'},
                                    'textAlign': 'center',
                                    'verticalAlign': 'middle',
                                },
                                # Center align all cells in 'group' column
                                {
                                    'if': {'column_id': 'group'},
                                    'textAlign': 'center',
                                    'verticalAlign': 'middle',
                                },
                                # Make font color blue if value is 'B' in 'group' column
                                {
                                    'if': {
                                        'filter_query': '{group} = "B"',
                                        'column_id': 'group'
                                    },
                                    'backgroundColor': "#052F49", 
                                    'color': "#CEE9F9",
                                    'fontWeight': 'bold',
                                },
                                # Make font color brown if value is 'B' in 'group' column
                                {
                                    'if': {
                                        'filter_query': '{group} = "C"',
                                        'column_id': 'group'
                                    },
                                    'backgroundColor': "#39301E", 
                                    'color': "#CEE9F9",
                                    'fontWeight': 'bold',
                                },
        ],
                        ),
                        
                    ], style=get_panel_style()),

# ...compare groups...
                    html.Div([
                        html.H2("Compare Model vs Whiteboard Groups", 
                                style={'textAlign': 'center', 'color': '#00bcd4'}),
                        dash_table.DataTable(
                            id='compare-groups-table',
                            data=compare_groups_df.to_dict('records'),
                            columns=[{"name": i, "id": i} for i in compare_groups_df.columns],
                            style_table=get_table_style(),
                            style_header={**get_table_header_style(), 'height': '100px'},
                            style_cell={**get_table_cell_style(), 'textAlign': 'center'},
                            cell_selectable=True,
                            style_cell_conditional=get_style_cell_conditional(compare_groups_df.columns),
                            style_data_conditional=[
                                # Highlight mismatches in 'comp' column
                                {
                                    'if': {
                                        'filter_query': '{comp} = "X"',
                                        'column_id': 'comp'
                                    },
                                    'backgroundColor': "#3C141F",
                                    'color':  "#F5C7D4",
                                    'fontWeight': 'bold'
                                    'horizontalAlignment'
                                },
                                # Optionally, highlight matches (empty string) in 'comp'
                                {
                                    'if': {
                                        'filter_query': '{comp} = ""',
                                        'column_id': 'comp'
                                    },
                                    'backgroundColor': "#111111",
                                    'color': 'white'
                                },
                            ],
                        ),
                    ], style=get_panel_style()),
                ],

#controls space betw panels
                style={
                    'display': 'flex',
                    'flexDirection': 'row',
                    'justifyContent': 'flex-start', 
                    'alignItems': 'flex-start',
                    'gap': '10px',
                    'width': '100%',
                }
            ),
        ],
        style={'backgroundColor': "#181616", 'padding': '20px'}
    )
    print("🔍 run_dash_app: Starting server...")



    def open_browser():
        try:
            webbrowser.get('firefox').open_new("http://127.0.0.1:8051/")
        except Exception as e:
            print(f"Could not open Firefox: {e}")
            # fallback to default browser
            try:
                webbrowser.open("http://127.0.0.1:8051/")
            except Exception as e2:
                print(f"Could not open default browser: {e2}")

    threading.Timer(1.5, open_browser).start()
    app.run_server(debug=False, port=8051)

if __name__ == "__main__":
    run_milk_dash_app()    