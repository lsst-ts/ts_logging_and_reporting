import logging

from lsst.ts.planning.tool import ZephyrInterface

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)


async def get_test_cases(
    keys: list[str],
    zephyr_token: str = None,
    jira_token: str = None,
    zephyr: ZephyrInterface | None = None,
) -> dict:
    """Retrieve Zephyr Scale test case names for a list of candidate keys.

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
    zephyr_token : `str`, optional
        Authentication token for Zephyr API access. Used when constructing
        a ``ZephyrInterface`` if one is not provided.
    jira_token : `str`, optional
        Authentication token for Jira API access. Used when constructing
        a ``ZephyrInterface`` if one is not provided.
    zephyr : `ZephyrInterface`, optional
        An existing Zephyr interface instance. If provided, this instance
        is used directly and token arguments are ignored. This is primarily
        intended for dependency injection and testing.

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
            zephyr_api_token=zephyr_token,
            jira_api_token=jira_token,
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
