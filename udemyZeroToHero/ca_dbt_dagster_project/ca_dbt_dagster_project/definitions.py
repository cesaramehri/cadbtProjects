from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import ca_dbt_airbnb_dbt_assets, dbtlearn_partitioned_dbt_assets
from .project import ca_dbt_airbnb_project
from .schedules import schedules

defs = Definitions(
    assets=[ca_dbt_airbnb_dbt_assets, dbtlearn_partitioned_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=ca_dbt_airbnb_project),
    },
)