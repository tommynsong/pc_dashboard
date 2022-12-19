from dash import dcc, html, Input, Output, callback, register_page
import dash_mantine_components as dmc
import plotly.express as px
import pandas as pd
import numpy

register_page(__name__, icon="fa:bar-chart")
#df = pd.read_parquet('../cache/discovery.parquet')
# df2 = df.groupby(['date_added', 'defended', 'service'])[
#    'defended'].count().reset_index(name='total')

#services = df2.service.unique()
#services = numpy.insert(services, 0, 'all')

# layout = html.Div(
#    [
#        dmc.Select(
#            id="dropdown",
#            data=[{"label": x, "value": x} for x in services],
#            value=services[0],
#            clearable=False,
#        ),
#        dcc.Graph(id="coverage"),
#    ]
#
# )


# @callback(Output("coverage", "figure"), Input("dropdown", "value"))
# def update_bar_chart(service):
#    if service == 'all':
#        mask = df2["service"] != service
#    else:
#        mask = df2["service"] == service
#    fig = px.bar(df2[mask], x="date_added", y="total",
#                 color="defended", barmode="group")
#    return fig
