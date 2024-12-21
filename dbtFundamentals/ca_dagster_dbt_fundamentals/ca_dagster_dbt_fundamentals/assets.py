from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import ca_dbt_dbtfundamentals_project


@dbt_assets(manifest=ca_dbt_dbtfundamentals_project.manifest_path)
def ca_dbt_dbtfundamentals_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    