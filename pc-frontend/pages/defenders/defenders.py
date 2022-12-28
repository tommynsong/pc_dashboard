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

redis_conn = DirectRedis(host='localhost', port=6379)
df2 = redis_conn.get('df_defenders')
all_versions = df2.version.unique()
all_accounts = df2.accountID.unique()
all_categories = df2.category.unique()

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
        dmc.Text("Category:"),
        dmc.MultiSelect(
            id="categories",
            placeholder="All",
            data=[{"value": x, "label": x} for x in all_categories],
            clearable=True,
        ),
        dcc.Graph(id="deployed"),
    ]
)


@callback(Output("deployed", "figure"),
          Input("accounts", "value"),
          Input("versions", "value"),
          Input("categories", "value"),
          )
def update_bar_chart(accounts, versions, categories):
    """Receives multiselect and applies filter to dataframe"""
    if accounts == None or len(accounts) == 0:
        accounts = []
        account_mask = ~df2["accountID"].isin(accounts)
    else:
        account_mask = df2["accountID"].isin(accounts)
    if versions == None or len(versions) == 0:
        versions = []
        version_mask = ~df2["version"].isin(versions)
    else:
        version_mask = df2["version"].isin(versions)
    if categories == None or len(categories) == 0:
        categories = []
        category_mask = ~df2["category"].isin(categories)
    else:
        category_mask = df2["category"].isin(categories)
    fig = px.bar(df2[(account_mask & version_mask & category_mask)], x="date_added", y="total",
                 color="category", barmode="stack")
    return fig
