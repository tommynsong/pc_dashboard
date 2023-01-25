'''
Duilds reporting page for Defender deployments
'''

import datetime
from dash import register_page, dcc, html, Input, Output, callback, dash_table
import dash_mantine_components as dmc
from direct_redis import DirectRedis
import plotly.express as px
import numpy

register_page(__name__, icon="fa:bar-chart")


def get_data():
    redis_conn = DirectRedis(host='redis-cache', port=6379)
    df = redis_conn.get('df_all_defenders')
    return df


df = get_data()


def get_multiselect(identifier, pick_list):
    multiselect = dmc.MultiSelect(
        id=identifier,
        placeholder='All',
        data=[{"value": x, "label": x} for x in pick_list],
        clearable=True,
    )
    return [multiselect]


layout = html.Div([
    html.Div(children=[
        dmc.Text("Version Selector"),
        html.Div(id='version_multiselect'),
        dmc.Space(h=20),
        dmc.Text("Account Selector"),
        html.Div(id='account_multiselect'),
        html.Div([
            dcc.Graph(id='historical_deployment'),
        ]),
        html.Div([
            dcc.Graph(id='deployed_by_account'),
        ]),
        html.Div(id='latest-timestamp', style={"padding": "20px"}),
        dcc.Interval(
            id='interval-component',
            interval=3600 * 1000,
            n_intervals=0
        ),
    ])
])


@ callback(
    [Output(component_id='latest-timestamp', component_property='children')],
    [Output(component_id='version_multiselect', component_property='children')],
    [Output(component_id='account_multiselect', component_property='children')],
    [Input('interval-component', 'n_intervals')]
)
def update_timestamp(interval):
    df = get_data()
    all_versions = numpy.sort(df.version.unique())
    all_accounts = numpy.sort(df.accountID.unique())
    version_multiselect = get_multiselect('versions', all_versions)
    account_multiselect = get_multiselect('accounts', all_accounts)
    timestamp = [html.Span(f"Last updated: {datetime.datetime.now()}")]
    return timestamp, version_multiselect, account_multiselect


@ callback(
    [Output(component_id='historical_deployment', component_property='figure')],
    [Output(component_id='deployed_by_account', component_property='figure')],
    [Input(component_id='accounts', component_property='value')],
    [Input(component_id='versions', component_property='value')],
)
def update_charts(accounts, versions):
    if accounts == None or len(accounts) == 0:
        accounts = []
        account_mask = ~df["accountID"].isin(accounts)
    else:
        account_mask = df["accountID"].isin(accounts)
    if versions == None or len(versions) == 0:
        versions = []
        version_mask = ~df["version"].isin(versions)
    else:
        version_mask = df["version"].isin(versions)
    df_historical = (df[(account_mask & version_mask)]).groupby(
        ['date_added', 'category'])['date_added'].count().reset_index(name='total')
    fig1 = px.bar(df_historical, x="date_added", y="total",
                  color="category", barmode="stack")
    date_mask = df['date_added'] == (df["date_added"].max())
    df_account_current = (df[(account_mask & version_mask & date_mask)]).groupby(
        ['accountID', 'category'])['accountID'].count().reset_index(name='total')
    fig2 = px.bar(df_account_current, x="accountID", y="total",
                  color="category", barmode="stack")
    return fig1, fig2
