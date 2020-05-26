from common.functions import *
import dash
from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
import geopandas as gpd
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests

with open('../selected_stops', 'r') as f:
    selected_stops = f.read().splitlines()

# stations JSON
file_path = 'm8_Estaciones.json'

if not os.path.exists(file_path):
    url = 'https://opendata.arcgis.com/datasets/19884a02ac044270b91fa478d80f7858_0.csv?outSR=%7B%22latestWkid%22%3A25830%2C%22wkid%22%3A25830%7D'
    r = requests.get(url)
    with open(file_path, 'w') as f:
        f.write(r.content.decode("utf-8"))

with open(file_path, 'r') as f:
    est = pd.read_csv(f)

# stations GeoJSON
file_path = 'm8_Estaciones.geojson'

if not os.path.exists(file_path):
    url = 'https://opendata.arcgis.com/datasets/19884a02ac044270b91fa478d80f7858_0.geojson'
    r = requests.get(url)
    with open(file_path, 'w') as f:
        f.write(r.content.decode("utf-8"))

est_geojson = gpd.read_file(file_path)
selected_est_geojson = est_geojson[est_geojson['IDESTACION']
                                       .isin(selected_stops)]


def get_stop_name(cod_stop):
    stop_row = est[est['IDESTACION'] == cod_stop].iloc[0]
    return str(stop_row['DENOMINACION'] +
               " (" + str(stop_row['CODIGOESTACION']) + ")")


app = dash.Dash(__name__)

cod_stops = sorted(selected_stops)

app.layout = html.Div(
    html.Div([
            dcc.Dropdown(
                id='from-dropdown',
                options=[{'label': get_stop_name(
                    cod_stop), 'value': cod_stop} for cod_stop in cod_stops],
                value='8_1336'
            ),
            dcc.Dropdown(
                id='to-dropdown',
                options=[{'label': get_stop_name(
                    cod_stop), 'value': cod_stop} for cod_stop in cod_stops],
                value='8_06277'
            ),
            dcc.Graph(id="boxplot"),
            dcc.Graph(id="map")
        ])
    )

@app.callback(Output("boxplot", "figure"), [Input('from-dropdown', 'value'),
                                            Input('to-dropdown', 'value')])
def make_figure(cod_stop_from, cod_stop_to):
    trip_times = get_running_times(cod_stop_from, cod_stop_to)
    if (trip_times.size <= 0):
        return 0
    else:
        fig = px.box(x=trip_times['departure_time'].dt.hour,
		y=trip_times['trip_time'].astype('timedelta64[s]')/60,
		color=trip_times['cod_line'],
		points="all",
		hover_data=[
		    trip_times['departure_time'].dt.day_name(),
		    trip_times['departure_time'].dt.minute,
		    trip_times['departure_time'].dt.day
		    ],
		)

        fig.update_layout(title='Running time through the day',
        	xaxis_title='Hour of the day',
        	yaxis_title='Running time (minutes)',
        	legend_title='Bus line')
        return fig


@app.callback(Output("map", "figure"), [Input('from-dropdown', 'value'),
                                        Input('to-dropdown', 'value')])
def make_figure(cod_stop_from, cod_stop_to):
    us_cities = pd.read_csv(
        "https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")

    fig = px.scatter_mapbox(
        selected_est_geojson,
        lat=selected_est_geojson['geometry'].y,
        lon=selected_est_geojson['geometry'].x,
        hover_data=['IDESTACION'],
        height=300,
        zoom=11
        )
    selected_est_geojson_from = selected_est_geojson[
        selected_est_geojson['IDESTACION'] == cod_stop_from].iloc[0]
    selected_est_geojson_to = selected_est_geojson[
        selected_est_geojson['IDESTACION'] == cod_stop_to].iloc[0]

    fig.add_trace(
        go.Scattermapbox(
                mode='markers',
                lat=[selected_est_geojson_from['geometry'].y],
                lon=[selected_est_geojson_from['geometry'].x],
                marker=go.scattermapbox.Marker(
                    size=17,
                    color='rgb(255, 0, 0)',
                    opacity=0.7
                ),
                hovertext= "From: " + cod_stop_from,
                hoverinfo= 'text',
                showlegend=False
            )
        )
    fig.add_trace(
        go.Scattermapbox(
                mode='markers',
                lat=[selected_est_geojson_to['geometry'].y],
                lon=[selected_est_geojson_to['geometry'].x],
                marker=go.scattermapbox.Marker(
                    size=17,
                    color='rgb(255, 0, 0)',
                    opacity=0.7
                ),
                hovertext= "To: " + cod_stop_to,
                hoverinfo= 'text',
                showlegend=False
            )
        )
    fig
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r": 0,"t":0,"l":0,"b":0})
    return fig

if __name__ == '__main__':
    app.run_server(host='0.0.0.0')
