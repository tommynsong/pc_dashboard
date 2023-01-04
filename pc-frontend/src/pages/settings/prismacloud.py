from dash import dcc, html, Input, Output, callback, register_page, State
import dash_mantine_components as dmc
import plotly.express as px
import numpy as np
import json
import requests
import datetime
from dash.exceptions import PreventUpdate



register_page(__name__, icon="fa:wrench")

n_intervals = 0

header = [
    html.Thead(
        html.Tr(
            [
                html.Th(""),
                html.Th("Current Values"),
                html.Th("New Values"),
            ]
        )
    )
]
row1 = html.Tr([
    html.Td("API URL"),
    html.Td(html.Div(id='pc_url')),
    html.Td(
        dmc.TextInput(
            id="new_pc_url",
            style={"width": 200},
            placeholder="https://api.prismacloud.io",
            type="url",
            required=True,
        ),
    ),
])
row2 = html.Tr([
    html.Td("API Key"),
    html.Td(html.Div(id='pc_key')),
    html.Td(
        dmc.TextInput(
            id="new_pc_key",
            style={"width": 200},
            required=True,
        ),
    ),
])
row3 = html.Tr([
    html.Td("API Secret"),
    html.Td(html.Div(id='pc_secret')),
    html.Td(
        dmc.TextInput(
            id="new_pc_secret",
            style={"width": 200},
            required=True,
        ),
    ),
])
row4 = html.Tr([html.Td(),html.Td(),html.Td()])
row5 = html.Tr([
    html.Td("Connection Status"),
    html.Td(html.Div(id='pc_status')),
    html.Td(
        dmc.Button(
            "Save",
            id="button"
        ),
    ),
])
row6 = html.Tr([
    html.Td("Last Checked"),
    html.Td(html.Div(id='timestamp')),
    html.Td(
        dcc.Location(id="url", refresh=True),
    ),
])
body = [html.Tbody([row1, row2, row3, row4, row5, row6])]

layout = html.Div([
    html.Div([
        dmc.Table(header + body),
        dcc.Interval(
            id='interval-component',
            interval=1*10000,
            n_intervals=0,
        )
    ]),
])

@callback(
    Output('timestamp', 'children'),
    Output('pc_url', 'children'),
    Output('pc_key', 'children'),
    Output('pc_secret', 'children'),
    Output('pc_status', 'children'),
    Input('interval-component', 'n_intervals'),
)
def update_url(n):
    if n < 0:
        raise PreventUpdate
    pc_status="Unsuccessful"
    response = requests.get('http://backend:5050/api/prismasettings')
    if response.text != '':
        settings = response.json()
        pc_url = settings["apiurl"]
        pc_key = settings["apikey"]
        pc_secret = settings["apisecret"]
        data = {
            "apiurl": pc_url,
            "apikey": pc_key,
            "apisecret": pc_secret
        }
        jsondata = json.dumps(data)
        response = requests.post(
            'http://backend:5050/api/prismastatus', json=jsondata
        )
        settings = response.json()
        if response.status_code == 200:
            pc_status="Success"
        pc_secret = "***************"
    else:
        pc_url = ""
        pc_key = ""
        pc_secret = ""
    return str(datetime.datetime.now()), pc_url, pc_key, pc_secret, pc_status

@callback(
    Output("url", "pathname"),
    Input("button", "n_clicks"),
    State("new_pc_url", "value"),
    State("new_pc_key", "value"),
    State("new_pc_secret", "value"),
)
def update_api_settings(n_clicks, new_pc_url, new_pc_key, new_pc_secret):
    if n_clicks is None:
        raise PreventUpdate
    else:
        if (new_pc_url and new_pc_key and new_pc_secret):
            data = {
                "apiurl": new_pc_url,
                "apikey": new_pc_key,
                "apisecret": new_pc_secret
            }
            jsondata = json.dumps(data)
            r = requests.post(
                'http://backend:5050/api/prismasettings', json=jsondata)
        return "settings/prismacloud"