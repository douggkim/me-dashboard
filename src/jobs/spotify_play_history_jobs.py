"""Dagster jobs and schedules for Spotify assets."""

import dagster as dg

from src.assets.spotify.spotify_play_history import spotify_play_history_bronze, spotify_play_history_silver

spotify_play_history_bronze_job = dg.define_asset_job(
    name="spotify_play_history_bronze_job",
    selection=[spotify_play_history_bronze],
    partitions_def=spotify_play_history_bronze.partitions_def,
)

spotify_play_history_silver_job = dg.define_asset_job(
    name="spotify_play_history_silver_job",
    selection=[spotify_play_history_silver],
    partitions_def=spotify_play_history_silver.partitions_def,
)


@dg.schedule(
    job=spotify_play_history_bronze_job, default_status=dg.DefaultScheduleStatus.RUNNING, cron_schedule="0 */3 * * *"
)
def spotify_play_history_bronze_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Schedule for materializing spotify_play_history_bronze asset every 3 hours.

    This schedule triggers at minute 0 of every 3rd hour (i.e., 00:00, 03:00, 06:00, etc.)
    and materializes the daily partition corresponding to the current date.

    Parameters
    ----------
    context : dg.ScheduleEvaluationContext
        The context object provided by Dagster containing schedule execution information.

    Returns
    -------
    dg.RunRequest
        Run request with the partition key set to the current date (YYYY-MM-DD).
    """
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(partition_key=scheduled_date)


@dg.schedule(
    job=spotify_play_history_silver_job, default_status=dg.DefaultScheduleStatus.RUNNING, cron_schedule="0 0,12 * * *"
)
def spotify_play_history_silver_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Schedule for materializing spotify_play_history_silver asset twice daily.

    This schedule triggers at midnight (00:00) and noon (12:00) each day and
    materializes the daily partition corresponding to the current date.

    Parameters
    ----------
    context : dg.ScheduleEvaluationContext
        The context object provided by Dagster containing schedule execution information.

    Returns
    -------
    dg.RunRequest
        Run request with the partition key set to the current date (YYYY-MM-DD).
    """
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(partition_key=scheduled_date)
