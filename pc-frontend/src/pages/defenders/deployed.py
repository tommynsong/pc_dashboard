"""
Builds reporting graph showing deployed defenders over time
"""
from dash import dcc, html, Input, Output, callback, register_page
import dash_mantine_components as dmc
import pandas as pd
import plotly.express as px
import numpy
from direct_redis import DirectRedis

register_page(__name__, icon="fa:bar-chart")

redis_conn = DirectRedis(host='cache', port=6379)
try:
    df = redis_conn.get('df_all_defenders')
except:
    data_list = [['','','','','']]
    df = pd.DataFrame(
        data_list, columns=['category', 'date_added', 'version', 'connected', 'accountID']) 
all_versions = numpy.sort(df.version.unique())
all_accounts = numpy.sort(df.accountID.unique())
layout = html.Div(
    [
        dmc.Text("Accounts:"),
        dmc.MultiSelect(
            id="accounts",
            placeholder="All",
            data=[{"value": x, "label": x} for x in all_accounts],
            clearable=True,
        ),
        dmc.Space(h=20),
        dmc.Text("Versions:"),
        dmc.MultiSelect(
            id="versions",
            placeholder="All",
            data=[{"value": x, "label": x} for x in all_versions],
            clearable=True,
        ),
        dmc.Space(h=20),
        dmc.Text("Historical Deployed Defenders"),
        dcc.Graph(id="historical_deployed"),
        dmc.Space(h=20),
        dmc.Text("Current Defenders By Account"),
        dcc.Graph(id="deployed_by_account"),
    ]
)


@callback(
    Output("historical_deployed", "figure"),
    Output("deployed_by_account", "figure"),
    Input("accounts", "value"),
    Input("versions", "value"),
)
def update_bar_chart(accounts, versions):
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
