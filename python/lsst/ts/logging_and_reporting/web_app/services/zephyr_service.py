import logging
import os

from lsst.ts.planning.tool import ZephyrInterface

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


async def get_test_cases(
    keys: list[str],
    zephyr: ZephyrInterface | None = None,
) -> dict:
    """
    Retrieve Zephyr Scale test case names for a list of candidate keys.
    For keys that include a suffix (e.g. ``_a``), only the parent
    portion (``BLOCK-T###``) is used when querying Zephyr Scale, as
    suffixed variants are not stored as independent test cases.
    Authentication credentials are read from the environment variables:
    ``JIRA_USERNAME``, ``JIRA_API_TOKEN``, and ``ZEPHYR_API_TOKEN``.
    Parameters
    ----------
    keys : `List[str]`
        List of Zephyr test case keys (e.g., ``BLOCK-T123`` or
        ``BLOCK-T123_a``).
    Returns
    -------
    `dict`
        A dictionary mapping each valid input key to its corresponding
        Zephyr test case name. Keys that fail retrieval are skipped.
    """
    logger.info(f"Getting Zephyr test case details for BLOCK keys {keys}")

    # Only construct real ZephyrInterface if not injected
    if zephyr is None:
        zephyr = ZephyrInterface(
            jira_username=os.environ.get("JIRA_USERNAME"),
            jira_api_token=os.environ.get("JIRA_API_TOKEN"),
            zephyr_api_token=os.environ.get("ZEPHYR_API_TOKEN"),
        )

    test_cases = {}

    for key in keys:
        try:
            # Test cases with _# at the end are represented in
            # Zephyr Scale without the _# at the end.
            parent_key = key.split("_", 1)[0]
            test_case_details = await zephyr.get_test_case(parent_key)
            test_cases[key] = test_case_details["name"]
        except Exception as e:
            logger.warning(f"Skipping Zephyr test case {key}: {e}")

    return test_cases
