from dash import Dash, html, dcc, Input, Output, State, register_page, callback, ctx
import dash_mantine_components as dmc
import datetime
import time
import requests
import json
from dash.exceptions import PreventUpdate

register_page(__name__, icon="fa:wrench")

layout = html.Div([
        html.Div(dmc.TextInput(
        id="api_url", label="API URL:", style={"width": 330},
        placeholder="https://api.prismacloud.io", value="")),
    html.Div(dmc.TextInput(
        id="api_key", label="API Key:", style={"width": 330},
        value="")),
    html.Div(dmc.PasswordInput(
        id="api_secret", label="API secret:", style={"width": 330},
        value="")),
    dmc.Space(h=30),
    dmc.Group([
        html.Div(dmc.Button("Clear", id='clear_button')),
        html.Div(dmc.Button("Load", id='load_button')),
        html.Div(dmc.Button("Test", id='test_button')),
        html.Div(dmc.Button("Save", id='save_button')),
    ]),
    dmc.Space(h=30),
    html.Div(id="status")
])

@callback(
    [Output('api_url', 'value')],
    [Output('api_key', 'value')],
    [Output('api_secret', 'value')],
    [Output('status', 'children')],
    [Input('clear_button', 'n_clicks')],
    [Input('load_button', 'n_clicks')],
    [Input('test_button', 'n_clicks')],
    [Input('save_button', 'n_clicks')],
    [State('api_url', 'value')],
    [State('api_key', 'value')],
    [State('api_secret', 'value')],
)
def load_data(clear_button, load_button, test_button, save_button, api_url, api_key, api_secret):
    msg = ''
    button_id = ctx.triggered_id if not None else 'No clicks yet'
    if button_id is None:
        raise PreventUpdate
    elif button_id == 'clear_button':
        return '','','',''
    elif button_id == 'load_button':
        try:
            response = requests.get('http://backend-api:5050/api/prismasettings')
            if response.status_code == 201:
                msg = "Successful Backend Connection"
                if response.text != '':
                    settings = response.json()
                    api_url = settings["apiurl"]
                    api_key = settings["apikey"]
                    api_secret = settings["apisecret"]
                else:
                    msg = "DB Contained no Saved Settings"
            else:
                msg = "Could not load settings"
        except:
            return '','','','Could not connect with Backend'
        return api_url, api_key, api_secret, msg
    elif button_id == 'test_button':
        jsondata = json.dumps({
            "apiurl": api_url,
            "apikey": api_key,
            "apisecret": api_secret
        })
        if api_url == '' or api_key == '' or api_secret == '':
            return api_url, api_key, api_secret, 'Complete all field entries'
        else:
            response = requests.post(
                'http://backend-api:5050/api/prismastatus', json=jsondata
        )
        if response.status_code == 200:
            return api_url, api_key, api_secret, 'Successful test'
        else:
            return api_url, api_key, api_secret, 'Unsuccessful test'
    elif button_id == 'save_button':
        jsondata = json.dumps({
            "apiurl": api_url,
            "apikey": api_key,
            "apisecret": api_secret
        })
        if api_url == '' or api_key == '' or api_secret == '':
            return api_url, api_key, api_secret, 'Complete all field entries'
        else:
            response = requests.post(
                'http://backend-api:5050/api/prismasettings', json=jsondata
        )
        if response.status_code == 201:
            return api_url, api_key, api_secret, 'Successful update'
        else:
            return api_url, api_key, api_secret, 'Unsuccessful update'
    else:
        return '','','','Unexpected input'