import dash
from dash import dcc
import flask
import dash_mantine_components as dmc
from dash_iconify import DashIconify
#import dash_auth

VALID_USERNAME_PASSWORD_PAIRS = {"prisma": "cloud"}

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server, use_pages=True,
                suppress_callback_exceptions=True)

#auth = dash_auth.BasicAuth(app, VALID_USERNAME_PASSWORD_PAIRS)


def create_nav_link(icon, label, href):
    return dcc.Link(
        dmc.Group(
            [
                dmc.ThemeIcon(
                    DashIconify(icon=icon, width=18),
                    size=30,
                    radius=30,
                    variant="light",
                ),
                dmc.Text(label, size="sm", color="gray"),
            ]
        ),
        href=href,
        style={"textDecoration": "none"},
    )


sidebar = dmc.Navbar(
    fixed=True,
    width={"base": 200},
    position={"top": 80},
    height=600,
    children=[
        dmc.ScrollArea(
            offsetScrollbars=True,
            type="scroll",
            children=[
                dmc.Group(
                    children=[
                        create_nav_link(
                            icon="radix-icons:rocket",
                            label="Home",
                            href="/",
                        ),
                    ],
                ),
                dmc.Divider(
                    label="Defenders", style={"marginBottom": 20, "marginTop": 20}
                ),
                dmc.Group(
                    children=[
                        create_nav_link(
                            icon=page["icon"], label=page["name"], href=page["path"]
                        )
                        for page in dash.page_registry.values()
                        if page["path"].startswith("/defenders")
                    ],
                ),
                dmc.Divider(
                    label="Vulnerabilities", style={"marginBottom": 20, "marginTop": 20}
                ),
                dmc.Group(
                    children=[
                        create_nav_link(
                            icon=page["icon"], label=page["name"], href=page["path"]
                        )
                        for page in dash.page_registry.values()
                        if page["path"].startswith("/vulnerabilities")
                    ],
                ),
                dmc.Divider(
                    label="Settings", style={"marginBottom": 20, "marginTop": 20}
                ),
                dmc.Group(
                    children=[
                        create_nav_link(
                            icon=page["icon"], label=page["name"], href=page["path"]
                        )
                        for page in dash.page_registry.values()
                        if page["path"].startswith("/settings")
                    ],
                ),
            ],
        )
    ],
)

app.layout = dmc.Container(
    [
        dmc.Header(
            height=70,
            children=[
                dmc.Image(
                    src="/assets/PrismaCloud.svg", alt="PrismaCloud", caption="PrismaCloud Logo", width=120
                )
            ],
            style={"backgroundColor": "#228be6"},
        ),
        sidebar,
        dmc.Container(
            dash.page_container,
            size="lg",
            pt=20,
            style={"marginLeft": 300},
        ),
    ],
    fluid=True,
)


if __name__ == "__main__":
    app.run_server(debug=True)
