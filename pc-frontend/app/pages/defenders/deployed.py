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
#df = pd.read_parquet('../cache/defender.parquet')
# df2 = df.groupby(['date_added', 'category', 'version'])[
#    'category'].count().reset_index(name='total')

redis_conn = DirectRedis(host='localhost', port=6379)
df2 = redis_conn.get('df_defenders')
versions = df2.version.unique()
versions = numpy.insert(versions, 0, 'all')

layout = html.Div(
    [
        dmc.Select(
            id="dropdown",
            data=[{"label": x, "value": x} for x in versions],
            value=versions[0],
            clearable=False,
        ),
        dcc.Graph(id="deployed"),
    ]
)


@callback(Output("deployed", "figure"), Input("dropdown", "value"))
def update_bar_chart(version):
    """Receives dropdown slection and applies filter to dataframe"""
    if version == 'all':
        mask = df2["version"] != version
    else:
        mask = df2["version"] == version
    fig = px.bar(df2[mask], x="date_added", y="total",
                 color="category", barmode="stack")
    return fig
