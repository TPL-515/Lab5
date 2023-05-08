from dagster import Definitions, load_assets_from_modules
from lab5.crud.sqlite import assets

sqlite_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=sqlite_assets,
)