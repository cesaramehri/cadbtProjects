from pathlib import Path

from dagster_dbt import DbtProject

ca_dbt_airbnb_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "ca_dbt_airbnb").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
ca_dbt_airbnb_project.prepare_if_dev()