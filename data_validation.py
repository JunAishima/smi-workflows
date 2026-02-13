from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from bluesky_tiled_plugins.writing.validator import validate
import time as ttime
from tiled.client import from_profile


@task
def check_stream(run):
    logger = get_run_logger()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")


@flow(retries=2, retry_delay_seconds=10)
def data_validation(uid, beamline_acronym="smi"):
    logger = get_run_logger()
    api_key = Secret.load("tiled-smi-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run_client = tiled_client[beamline_acronym]["migration"][uid]
    run_client_raw = tiled_client[beamline_acronym]["raw"][uid]
    logger.info(f"Validating uid {uid}")
    start_time = ttime.monotonic()
    check_stream(run_client_raw)
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"Finished checking raw stream; {elapsed_time = }")
    validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"Finished validating data; total {elapsed_time = }")
