from pathlib import Path

from dagster import AssetExecutionContext, Definitions, load_assets_from_modules, ExperimentalWarning, DefaultScheduleStatus

from dagster_dbt import (
    DbtCliResource,
    DbtProject,
    build_schedule_from_dbt_selection,
    dbt_assets,
)


import warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning) 

from dags.assets import stg_films, stg_people, stg_planets, stg_species, stg_vehicles
staging_assets = load_assets_from_modules([stg_films, stg_people, stg_planets, stg_species, stg_vehicles])


RELATIVE_PATH_TO_MY_DBT_PROJECT = "../dbts"

my_project = DbtProject(
    project_dir=Path(__file__)
    .joinpath("..", RELATIVE_PATH_TO_MY_DBT_PROJECT)
    .resolve(),
)


@dbt_assets(manifest=my_project.manifest_path)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


dbt_assets_schedule = build_schedule_from_dbt_selection(
    [my_dbt_assets],
    job_name="materialize_dbt_models",
    cron_schedule="10/30 * * * *",
    dbt_select="fqn:*",
    default_status=DefaultScheduleStatus.RUNNING
)

defs = Definitions(
    assets=staging_assets + [my_dbt_assets],
    schedules=[dbt_assets_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=my_project),
    },
)
