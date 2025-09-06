
import webbrowser
from dash import Dash, html, dash_table
import pandas as pd
from insem_functions.ipiv import Ipiv
from insem_functions.insem_ultra_basics import InsemUltraBasics
from insem_functions.insem_ultra_data import InsemUltraData
from milk_basics import MilkBasics
from status_functions.statusData2 import StatusData2


class IpivDashApp:
    def __init__(self, insem_ultra_basics=None, insem_ultra_data=None, 
                 milk_basics=None, status_data2=None):
        self.ipiv = Ipiv(
            insem_ultra_basics=insem_ultra_basics or InsemUltraBasics(),
            insem_ultra_data=insem_ultra_data or InsemUltraData(),
            milk_basics=milk_basics or MilkBasics(),
            status_data2=status_data2 or StatusData2()
        )
        
        self.ipiv_milkers_df = self.ipiv.ipiv_milkers.reset_index(drop=True)
        
        
        # Dynamically set column widths
        cols = list(self.ipiv_milkers_df.columns)
        self.COLUMN_WIDTHS = {
            cols[0]: '70px',    # WY_id
            cols[1]: '70px',    # lact#
        }
        # Set wider width for all remaining columns
        for col in cols[2:]:
            self.COLUMN_WIDTHS[col] = '120px'





    def get_style_cell_conditional(self, table_columns):
        return [
            {
                'if': {'column_id': table_columns[0]},
                'width': self.COLUMN_WIDTHS.get(table_columns[0], '70px'),
                'textAlign': 'center',
                'verticalAlign': 'middle'
            },
            {
                'if': {'column_id': table_columns[1]},
                'width': self.COLUMN_WIDTHS.get(table_columns[1], '70px'),
                'textAlign': 'center',
                'verticalAlign': 'middle'
            },
            *[
                {
                    'if': {'column_id': col},
                    'width': self.COLUMN_WIDTHS.get(col, '120px')
                }
                for col in table_columns[2:]
            ]
        ]
        
    def run(self):
        app = Dash(__name__)

        app.layout = html.Div([
            html.H1(
                f"🐄 Insemination dates for : {pd.Timestamp.now().strftime('%Y-%m-%d')}",
                style={'textAlign': 'center', 'marginBottom': '20px',
                       'fontSize': '32px', 'color': '#333'}
            ),
            html.Div([
                html.Div([
                    html.H2("Ipiv All Basic", style={'textAlign': 'center', 'color': '#00bcd4'}),
                    dash_table.DataTable(
                        id='ipiv-all-basic-table',
                        data=self.ipiv_milkers_df.to_dict('records'),
                        columns=[{"name": i, "id": i} for i in self.ipiv_milkers_df.columns],
                        style_table={'overflowX': 'auto', 'maxWidth': '100%'},
                        style_header={'backgroundColor': "#1D2121", 'color': "#a9d0ce", 'fontWeight': 'bold', 'textAlign': 'center', 'fontSize': '22px', 'height': '60px', 'verticalAlign': 'middle'},
                        style_cell={'fontFamily': 'Courier New, monospace', 'fontSize': '18px', 'backgroundColor': "#1D2121", 'color': "#cdfffd", 'padding': '2px 4px', 'whiteSpace': 'normal', 'minHeight': '30px', 'border': '1px solid #777878'},
                        cell_selectable=True,
                        style_cell_conditional=self.get_style_cell_conditional(self.ipiv_milkers_df.columns),
                    ),
                ], style={'border': '3px solid #00bcd4', 
                          'borderRadius': '15px', 
                          'padding': '18px', 
                          'margin': '10px', 
                          'backgroundColor': '#1D2121', 
                          'boxShadow': '2px 2px 12px #222', 
                          'flex': '1 1 0', 
                          'minWidth': '350px', 
                          'maxWidth': '100%', 
                          'overflowX': 'auto'}),
            ])
        ])

        webbrowser.open("http://127.0.0.1:8050/")
        app.run_server(debug=True, port=8050)

if __name__ == "__main__":
    IpivDashApp().run()