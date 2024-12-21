from pathlib import Path

from dagster_dbt import DbtProject

ca_dbt_dbtfundamentals_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "ca_dbt_dbtfundamentals").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
ca_dbt_dbtfundamentals_project.prepare_if_dev()