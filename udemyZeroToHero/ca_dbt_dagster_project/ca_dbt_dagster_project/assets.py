from dagster import (AssetExecutionContext, DailyPartitionsDefinition, OpExecutionContext)
from dagster_dbt import (DagsterDbtTranslator, DbtCliResource, dbt_assets, default_metadata_from_dbt_resource_props)
from .project import ca_dbt_airbnb_project
import json
from typing import Any, Mapping



# Original
@dbt_assets(manifest=ca_dbt_airbnb_project.manifest_path, exclude="silver_fact_reviews")
def ca_dbt_airbnb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()



# Additional: for incremental loading using partitions from dagster
# many dbt assets use an incremental approach to avoid
# re-processing all data on each run
# this approach can be modelled in dagster using partitions
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = {"partition_expr": "date"}
        default_metadata = default_metadata_from_dbt_resource_props(dbt_resource_props)
        return {**default_metadata, **metadata}


@dbt_assets(manifest=ca_dbt_airbnb_project.manifest_path, 
            select="silver_fact_reviews",
            partitions_def=daily_partitions,
            dagster_dbt_translator=CustomDagsterDbtTranslator())
def dbtlearn_partitioned_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    (
        first_partition,
        last_partition,
    ) = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )
    dbt_vars = {"start_date": str(first_partition), "end_date": str(last_partition)}
    dbt_args = ["build", "--vars", json.dumps(dbt_vars)]
    dbt_cli_task = dbt.cli(dbt_args, context=context, raise_on_error=False)
    
    yield from dbt_cli_task.stream()