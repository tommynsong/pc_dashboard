from dash import dcc, register_page
import dash_mantine_components as dmc

register_page(__name__, path="/", icon="fa-solid:home")

layout = dmc.Container(
    [
        dmc.Title("Welcome to the Prisma Cloud Reporting page"),
        dcc.Markdown(
            """
            Fun with Prisma Cloud APIs and Flask.
            
            Report Breakdown:
            ```
            - Home
            - Defenders
                |-- Coverage   # Shows current deployed Defenders and Coverage Gaps
                |-- Deployed   # Shows current Defender deployment by version, type, and account
            - Vulnerabilities
                |-- Registries # Shows historical vulnerability details by Repo and tag within Registries
            ```
            
            Offered without warranty.
            """
        ),
    ]
)
