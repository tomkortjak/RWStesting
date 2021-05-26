import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd

import dash
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}


class GraphPlot:
    def __init__(self, df, stats):
        self.default_df = df
        self.df = df
        self.stats = stats
        self.app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
        self.setup_layout()
        self.setup_callbacks()
        self.app.run_server(debug=False)

    def setup_layout(self):
        self.app.layout = html.Div(children=[
            html.H1(children='Dashboard RWS Team-4'),
            html.Div(children='Een basis concept van het dasboard voor de visualisatie van RDW data door team 4.'),
            html.Div(
                children="LET OP!: Auto's waarvan de benodigde informatie ontbreekt, worden niet getoond in sommige graphs."),
            dcc.Tabs(id="tabs-styled-with-inline", value='tab-1', children=[
                dcc.Tab(label='Algemene statistieken', value='tab-1', style=tab_style,
                        selected_style=tab_selected_style),
                dcc.Tab(label="Groei verkochte auto's", value='tab-2', style=tab_style,
                        selected_style=tab_selected_style),
                dcc.Tab(label="Aantallen brandstoftypes", value='tab-3', style=tab_style,
                        selected_style=tab_selected_style),
            ], style=tabs_styles),
            html.Div(id='tabs-content-inline')
        ], style={'columnCount': 1})

    def setup_callbacks(self):
        @self.app.callback(
            dash.dependencies.Output('output-container-price-slider', 'children'),
            [dash.dependencies.Input('price-slider', 'value')])
        def update_price_slider(value):
            return 'Prijs categorie: "{}"'.format(value)

        @self.app.callback(
            dash.dependencies.Output('output-container-battery-capacity-slider', 'children'),
            [dash.dependencies.Input('battery-capacity-slider', 'value')])
        def update_battery_slider(value):
            return 'Battery capacity: "{}"'.format(value)

        @self.app.callback(
            dash.dependencies.Output('output-container-price-slider-2', 'children'),
            [dash.dependencies.Input('price-slider-2', 'value')])
        def update_price_slider(value):
            return 'Prijs categorie: "{}"'.format(value)

        @self.app.callback(
            dash.dependencies.Output('output-container-battery-capacity-slider-2', 'children'),
            [dash.dependencies.Input('battery-capacity-slider-2', 'value')])
        def update_battery_slider(value):
            return 'Battery capacity: "{}"'.format(value)

        @self.app.callback(
            dash.dependencies.Output('total-filtered-cars', 'children'),
            dash.dependencies.Output('price-graph', 'figure'),
            dash.dependencies.Output('battery-cap-graph', 'figure'),
            dash.dependencies.Output('charging-speed-graph', 'figure'),
            dash.dependencies.Output('efficiency-graph', 'figure'),
            [dash.dependencies.Input('price-slider', 'value'),
             dash.dependencies.Input('battery-capacity-slider', 'value'),
             dash.dependencies.Input('merk-input', 'value'),
             dash.dependencies.Input('soort-input', 'value'),
             dash.dependencies.Input('kenteken-input', 'value')])
        def update_graphs_tab1(price_range, battery_range, merk, soort, kenteken):
            # filters
            bool_series = self.df['price($)'].between(price_range[0], price_range[1])
            filtered_df = self.df[bool_series]
            bool_series = filtered_df['battery_pack_capacity(kWh)'].between(battery_range[0], battery_range[1])
            filtered_df = filtered_df[bool_series]
            if merk is not None:
                filtered_df = filtered_df[filtered_df['merk'].str.contains(merk)]
            if kenteken is not None:
                filtered_df = filtered_df[filtered_df['kenteken'].str.contains(kenteken)]
            if soort is not None:
                filtered_df = filtered_df.loc[filtered_df['voertuigsoort'].isin(soort)]

            # Remove duplicates to increase performance
            filtered_df.drop_duplicates(subset=['auto'], inplace=True)

            if len(filtered_df) != 0:
                # Graphs
                price_graph = px.bar(filtered_df, x='auto', y='price($)',
                                     color="auto",
                                     barmode="overlay", title="Gemiddelde prijs per auto")

                battery_cap_graph = px.bar(filtered_df, x="auto", y="battery_pack_capacity(kWh)",
                                           color="auto", barmode="overlay",
                                           title="Batterij capaciteit per auto")

                charging_speed_graph = px.bar(filtered_df, x="auto", y="avg_charging_speed(DC)(km/h)",
                                              color="auto", barmode="overlay",
                                              title="Oplaadsnelheid per auto")

                efficiency_graph = px.bar(filtered_df, x="auto", y="efficiency(kWh/100km)",
                                          color="auto", barmode="overlay",
                                          title="Efficiëntie per auto")
            else:
                empty_graph = go.Figure().add_annotation(
                    text="No matching data found",
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(
                        family="Courier New, monospace",
                        size=21,
                        color="#000000"
                    ),
                )
                price_graph = empty_graph
                battery_cap_graph = empty_graph
                charging_speed_graph = empty_graph
                efficiency_graph = empty_graph
            return len(filtered_df), price_graph, battery_cap_graph, charging_speed_graph, efficiency_graph

        # Filter is not DRY because duplicate ID's cause dashboard to crash
        @self.app.callback(
            dash.dependencies.Output('total-filtered-cars-2', 'children'),
            dash.dependencies.Output('sold-cars-graph', 'figure'),
            dash.dependencies.Output('sold-batteries-graph', 'figure'),
            dash.dependencies.Output('sold-charge-graph', 'figure'),
            dash.dependencies.Output('sold-efficiency-graph', 'figure'),
            [dash.dependencies.Input('price-slider-2', 'value'),
             dash.dependencies.Input('battery-capacity-slider-2', 'value'),
             dash.dependencies.Input('merk-input-2', 'value'),
             dash.dependencies.Input('soort-input-2', 'value'),
             dash.dependencies.Input('kenteken-input-2', 'value')])
        def update_graphs_tab2(price_range, battery_range, merk, soort, kenteken):
            # Filters
            bool_series = self.df['price($)'].between(price_range[0], price_range[1])
            filtered_df = self.df[bool_series]
            bool_series = filtered_df['battery_pack_capacity(kWh)'].between(battery_range[0], battery_range[1])
            filtered_df = filtered_df[bool_series]
            if merk is not None:
                filtered_df = filtered_df[filtered_df['merk'].str.contains(merk)]
            if kenteken is not None:
                filtered_df = filtered_df[filtered_df['kenteken'].str.contains(kenteken)]
            if soort is not None:
                filtered_df = filtered_df.loc[filtered_df['voertuigsoort'].isin(soort)]

            if len(filtered_df) != 0:
                # Reformating the date
                filtered_df['datum_eerste_afgifte_nederland'] = pd.to_datetime(
                    filtered_df['datum_eerste_afgifte_nederland'],
                    format='%d/%m/%y',
                    exact=False)
                filtered_df['datum'] = filtered_df['datum_eerste_afgifte_nederland'].map(lambda x: x.strftime('%Y/%m'))
                sold_cars_df = filtered_df.groupby('datum').size().to_frame(name='aantal_verkocht').reset_index()
                sold_cars_df['datum'] = pd.to_datetime(
                    sold_cars_df['datum'],
                    format='%Y/%m',
                    exact=False)
                filtered_df = filtered_df.groupby('datum').mean().reset_index()
                filtered_df['datum'] = pd.to_datetime(
                    filtered_df['datum'],
                    format='%Y/%m',
                    exact=False)

                # Graphs
                sold_cars_graph = px.bar(sold_cars_df, x="datum", y="aantal_verkocht", barmode="group",
                                         title="Verkochte auto's per maand") \
                    .update_xaxes(tickangle=45)
                sold_cars_batteries_graph = px.bar(filtered_df, x="datum", y="battery_pack_capacity(kWh)",
                                                   barmode="group",
                                                   title="Batterij capaciteit verkochte auto's per maand") \
                    .update_xaxes(tickangle=45)
                sold_cars_charge_speed_graph = px.bar(filtered_df, x="datum", y="avg_charging_speed(DC)(km/h)",
                                                      barmode="group",
                                                      title="Oplaadsnelheid capaciteit verkochte auto's per maand") \
                    .update_xaxes(tickangle=45)

                sold_cars_efficiency_graph = px.bar(filtered_df, x="datum", y="efficiency(kWh/100km)",
                                                    barmode="group",
                                                    title="Efficiëntie capaciteit verkochte auto's per maand") \
                    .update_xaxes(tickangle=45)
            else:
                empty_graph = go.Figure().add_annotation(
                    text="No matching data found",
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(
                        family="Courier New, monospace",
                        size=21,
                        color="#000000"
                    ),
                )
                sold_cars_graph = empty_graph
                sold_cars_batteries_graph = empty_graph
                sold_cars_charge_speed_graph = empty_graph
                sold_cars_efficiency_graph = empty_graph
            return len(filtered_df), sold_cars_graph, sold_cars_batteries_graph, \
                   sold_cars_charge_speed_graph, sold_cars_efficiency_graph

        def get_sub_graphs_fig():
            print(self.stats.head())
            fig = make_subplots(rows=2, cols=2,
                                subplot_titles=(
                                    "Benzine", "Diesel", "Elektriciteit", "Aantal huidige auto's"),
                                specs=[[{"type": "scatter"}, {"type": "scatter"}],
                                       [{"type": "scatter"}, {"type": "pie"}]])
            fig.add_trace(
                go.Scatter(x=self.stats["datum"], y=self.stats['benzine']),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=self.stats["datum"], y=self.stats['diesel']),
                row=1, col=2
            )
            fig.add_trace(
                go.Scatter(x=self.stats["datum"], y=self.stats['elektriciteit']),
                row=2, col=1
            )

            # We get the latest row and top gas types
            current = self.stats[self.stats.datum == self.stats.datum.max()]
            labels = ["Benzine", "Diesel", "Elektriciteit"]
            values = [current.benzine.max(), current.diesel.max(), current.elektriciteit.max()]
            fig.add_trace(
                go.Pie(labels=labels, values=values, textinfo='label+percent'),
                row=2, col=2
            )

            fig.update_layout(height=850, title_text="Aantal auto's per gassoort")
            return fig

        @self.app.callback(dash.dependencies.Output('tabs-content-inline', 'children'),
                           [dash.dependencies.Input('tabs-styled-with-inline', 'value')])
        def render_content(tab):
            if tab == 'tab-1':
                return html.Div([
                    html.Div(children=[
                        html.Div(children=[
                            html.Div(children=[
                                html.Label('Auto merk: '),
                                dcc.Dropdown(id='merk-input',
                                             options=[
                                                 {"label": merk, "value": merk} for merk in self.df['merk'].unique()
                                             ])]),
                        ], style={'columnCount': 1, 'margin': '0 10px 10px 10px'}),
                        html.Div(children=[
                            html.Div(children=[
                                html.Label('Auto model: '),
                                dcc.Input(id='model-input', value='', type='text',
                                          placeholder='voer handelsbenaming in'),
                            ]),
                            html.Div(children=[
                                html.Label('Kenteken: '),
                                dcc.Input(id='kenteken-input', value='', type='text', placeholder='voer kenteken in'),
                            ])
                        ], style={"columnCount": 3, 'margin': '10px'}),
                        html.Div(children=[
                            html.Label('Batterij capaciteit(kWh):'),
                            dcc.RangeSlider(
                                id='battery-capacity-slider',
                                min=0,
                                max=self.default_df['battery_pack_capacity(kWh)'].max(),
                                value=[0,
                                       self.default_df['battery_pack_capacity(kWh)'].max()],
                            ),
                            html.Div(id='output-container-battery-capacity-slider')
                        ]),
                        html.Div(children=[
                            html.Label('Voertuigsoort: '),
                            dcc.Checklist(
                                id='soort-input',
                                options=[
                                    {'label': 'Personenauto', 'value': 'Personenauto'},
                                    {'label': 'Bedrijfsauto', 'value': 'Bedrijfsauto'},
                                ]
                            ),
                        ], style={'display': 'inline-block', 'margin-bottom': '80px'}),
                        html.Div(children=[
                            html.Label('Catalogusprijs in $:'),
                            dcc.RangeSlider(
                                id='price-slider',
                                min=0,
                                max=self.default_df['price($)'].max(),
                                value=[0, self.default_df['price($)'].max()],
                                marks={
                                    0: '0',
                                    int(self.default_df['price($)'].max()): repr(self.default_df['price($)'].max())
                                }
                            ),
                        ]),
                    ], style={'columnCount': 2, 'margin': 0}),
                    html.Div(children=[
                        html.Label('Current amount of filtered models:'),
                        html.Div(id='total-filtered-cars')
                    ]),
                    html.Div(children=[
                        dcc.Graph(id='price-graph'),
                        dcc.Graph(id='battery-cap-graph'),
                    ], style={'columnCount': 2, 'margin': 0}),
                    html.Div(children=[
                        dcc.Graph(id='charging-speed-graph'),
                        dcc.Graph(id='efficiency-graph'),
                    ], style={'columnCount': 2, 'margin': 0})
                ])
            elif tab == 'tab-2':
                return html.Div([
                    html.Div(children=[
                        html.Div(children=[
                            html.Div(children=[
                                html.Label('Auto merk: '),
                                dcc.Dropdown(id='merk-input-2',
                                             options=[
                                                 {"label": merk, "value": merk} for merk in self.df['merk'].unique()
                                             ])]),
                        ], style={'columnCount': 1, 'margin': '0 10px 10px 10px'}),
                        html.Div(children=[
                            html.Div(children=[
                                html.Label('Auto model: '),
                                dcc.Input(id='model-input-2', value='', type='text',
                                          placeholder='voer handelsbenaming in'),
                            ]),
                            html.Div(children=[
                                html.Label('Kenteken: '),
                                dcc.Input(id='kenteken-input-2', value='', type='text', placeholder='voer kenteken in'),
                            ])
                        ], style={"columnCount": 3, 'margin': '10px'}),
                        html.Div(children=[
                            html.Label('Batterij capaciteit(kWh):'),
                            dcc.RangeSlider(
                                id='battery-capacity-slider-2',
                                min=0,
                                max=self.default_df['battery_pack_capacity(kWh)'].max(),
                                value=[0,
                                       self.default_df['battery_pack_capacity(kWh)'].max()],
                            ),
                            html.Div(id='output-container-battery-capacity-slider-2')
                        ]),
                        html.Div(children=[
                            html.Label('Voertuigsoort: '),
                            dcc.Checklist(
                                id='soort-input-2',
                                options=[
                                    {'label': 'Personenauto', 'value': 'Personenauto'},
                                    {'label': 'Bedrijfsauto', 'value': 'Bedrijfsauto'}
                                ]
                            ),
                        ], style={'display': 'inline-block', 'margin-bottom': '80px'}),
                        html.Div(children=[
                            html.Label('Catalogusprijs in $:'),
                            dcc.RangeSlider(
                                id='price-slider-2',
                                min=0,
                                max=self.default_df['price($)'].max(),
                                value=[0, self.default_df['price($)'].max()],
                                marks={
                                    0: '0',
                                    int(self.default_df['price($)'].max()): repr(self.default_df['price($)'].max())
                                }
                            ),
                            html.Div(id='output-container-price-slider-2')
                        ]),
                    ], style={'columnCount': 2, 'margin': 0}),
                    html.Div(children=[
                        html.Label('Current amount of months:'),
                        html.Div(id='total-filtered-cars-2')
                    ]),
                    html.Div(children=[
                        dcc.Graph(id='sold-cars-graph'),
                        dcc.Graph(id='sold-batteries-graph'),
                    ], style={'columnCount': 2, 'margin': 0}),
                    html.Div(children=[
                        dcc.Graph(id='sold-charge-graph'),
                        dcc.Graph(id='sold-efficiency-graph')
                    ], style={'columnCount': 2, 'margin': 0}),
                ])
            elif tab == 'tab-3':
                sub_plots = get_sub_graphs_fig()
                return html.Div([
                    dcc.Graph(figure=sub_plots),
                ])
