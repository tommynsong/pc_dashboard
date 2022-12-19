from dash import dcc, register_page
import dash_mantine_components as dmc

register_page(__name__, path="/", icon="fa-solid:home")

layout = dmc.Container(
    [
        dmc.Title("Welcome to the home page"),
        dcc.Markdown(
            """
            ```
            Reportining dashboard for basic Compute metrics.
            No support.
            ``` 
            
            """
        ),
    ]
)
