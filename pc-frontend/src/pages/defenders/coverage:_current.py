'''
Duilds reporting page for Defender deployments
'''

from dash import register_page, html, dash_table
from direct_redis import DirectRedis

register_page(__name__, icon="fa:table")


def get_data():
    redis_conn = DirectRedis(host='redis-cache', port=6379)
    df = redis_conn.get('curr_coverage')
    return df


df = get_data()
df.drop('date_added', axis=1, inplace=True)


layout = html.Div([
    dash_table.DataTable(
        id='datatable-interactivity',
        columns=[
            {"name": i, "id": i, "deletable": False, "selectable": True} for i in df.columns
        ],
        data=df.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        page_action="native",
        page_current=0,
        page_size=25,
        export_format="csv",
    ),
    html.Div(id='datatable-interactivity-container')
])
