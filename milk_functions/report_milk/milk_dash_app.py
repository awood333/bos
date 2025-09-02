from dash import Dash, html, dash_table
import webbrowser
import pandas as pd
from milk_functions.report_milk.report_milk import ReportMilk



COLUMN_WIDTHS = {
    'WY_id': '70px',
    'avg': "100px",
    'AM': '50px',
    'PM': '50px',
    'pct chg from avg': '100px',
    'days milking' : '100px',
    'u_read' : "70px",
    'expected bdate' : "150px",
    'group' : "70px",
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
        'flex': '1 1 0',
        'minWidth': '350px',
        'maxWidth': '100%',
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
        'fontSize': '18px',
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
      
def run_dash_app(milk_aggregates=None, milking_groups=None):
        
    report = ReportMilk(milk_aggregates=milk_aggregates, milking_groups=milking_groups)
    tenday_df, halfday_df, groups_df = report.tenday, report.halfday, report.groups

    
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
                            style_cell_conditional=get_style_cell_conditional(tenday_df.columns),
                            style_data_conditional=[
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
                    ], style={**get_panel_style(), 'maxWidth': '350px', 'minWidth': '350px', 'flex': '0 0 350px'}),

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
                                # Center align all cells in 'group' column
                                {
                                    'if': {'column_id': 'group'},
                                    'textAlign': 'center',
                                    'verticalAlign': 'middle',
                                },
                                # Make font color red if value is 'B' in 'group' column
                                {
                                    'if': {
                                        'filter_query': '{group} = "B"',
                                        'column_id': 'group'
                                    },
                                    'backgroundColor': "#052F49", 
                                    'color': "#CEE9F9",
                                    'fontWeight': 'bold',
                                },
        ],
                        ),
                        
                    ], style={**get_panel_style(), 'maxWidth': '800px', 'minWidth': '800px', 'flex': '0 0 800px'}),
                ],
                style={
                    'display': 'flex',
                    'flexDirection': 'row',
                    'justifyContent': 'space-between',
                    'alignItems': 'flex-start',
                    'gap': '10px',
                    'width': '100%',
                }
            ),
        ],
        style={'backgroundColor': "#181616", 'padding': '20px'}
    )

    webbrowser.open("http://127.0.0.1:8050/")
    # if __name__ == "__main__":
    app.run_server(debug=False, port=8050)