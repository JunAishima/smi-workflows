from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from bluesky_tiled_plugins.writing.validator import validate
import time as ttime
from tiled.client import from_profile


@flow(retries=2, retry_delay_seconds=10)
def data_validation(uid, beamline_acronym="smi"):
    logger = get_run_logger()
    api_key = Secret.load("tiled-smi-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run_client = tiled_client[beamline_acronym]["raw"][uid]
    logger.info(f"Validating uid {uid}")
    start_time = ttime.monotonic()
    validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"Finished validating data; {elapsed_time = }")
