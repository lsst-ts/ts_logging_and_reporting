import logging
import os
import re

from lsst.ts.planning.tool import ZephyrInterface

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)

# Zephyr Scale keys have the form BLOCK-T###[_#]
VALID_KEY_RE = re.compile(r"^BLOCK-T\d+(?:_[A-Za-z0-9]+)?$")


async def get_test_cases(keys: list[str]) -> dict:
    """
    Retrieve Zephyr Scale test case names for a list of candidate keys.

    Filters the provided keys against `VALID_KEY_RE` (expected format:
    ``BLOCK-T###`` or ``BLOCK-T###_<suffix>``). For keys that include a
    suffix (e.g. ``_a``), only the parent portion (``BLOCK-T###``) is used
    when querying Zephyr Scale, as suffixed variants are not stored as
    independent test cases.

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
        Zephyr test case name. Invalid keys or keys that fail retrieval
        are skipped.
    """
    valid_keys = [k for k in keys if VALID_KEY_RE.match(k)]
    logger.info(f"Getting Zephyr test case details for valid keys {valid_keys}")
    zephyr = ZephyrInterface(
        jira_username=f"{os.environ.get('JIRA_USERNAME')}",
        jira_api_token=f"{os.environ.get('JIRA_API_TOKEN')}",
        zephyr_api_token=f"{os.environ.get('ZEPHYR_API_TOKEN')}",
    )
    test_cases = {}
    for key in valid_keys:
        try:
            # Test cases with _# at the end are represented in
            # Zephyr Scale without the _# at the end.
            parent_key = key.split("_", 1)[0]
            test_case_details = await zephyr.get_test_case(parent_key)
            test_cases[key] = test_case_details["name"]
        except Exception as e:
            logger.warning(f"Skipping Zephyr test case {key}: {e}")

    print(test_cases)
    return test_cases
