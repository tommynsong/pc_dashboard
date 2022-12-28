"""
Builds reporting graph showing deployed defenders over time
"""
from dash import dcc, html, Input, Output, callback, register_page
import dash_mantine_components as dmc
import pandas
import plotly.express as px
import numpy
from direct_redis import DirectRedis

register_page(__name__, icon="fa:bar-chart")

redis_conn = DirectRedis(host='localhost', port=6379)
df2 = redis_conn.get('df_defenders')
all_versions = df2.version.unique()
all_accounts = df2.accountID.unique()

layout = html.Div(
    [
        dmc.Text("Accounts:"),
        dmc.MultiSelect(
            id="accounts",
            placeholder="All",
            value=all_accounts[0],
            data=[{"value": x, "label": x} for x in all_accounts],
            clearable=True,
        ),
        dmc.Space(h=20),
        dmc.Text("Versions:"),
        dmc.MultiSelect(
            id="values",
            placeholder="All",
            value=all_versions[0],
            data=[{"value": x, "label": x} for x in all_versions],
            clearable=True,
        ),
        dcc.Graph(id="deployed"),
    ]
)


@callback(Output("deployed", "figure"), [Input("accounts", "value"), Input("versions", "value")])
def update_bar_chart(accounts, versions):
    """Receives dropdown slection and applies filter to dataframe"""
    print(versions)
    if versions == 'all':
        mask = df2["version"] != versions
    else:
        mask = df2["version"] == versions
        print(df2[mask])
    fig = px.bar(df2[mask], x="date_added", y="total",
                 color="category", barmode="stack")
    return fig
