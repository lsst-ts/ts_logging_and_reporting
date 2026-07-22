from lsst.ts.logging_and_reporting.utils.misc import (
    JIRA_BLOCK_BASE_URL,
    ZEPHYR_BLOCK_BASE_URL,
    build_block_response,
)


# Test the constructor that unifies responses from Jira and Zephyr
# for BLOCK details.
def test_build_block_response_combines_sources():
    zephyr_data = {
        "BLOCK-T123": "Zephyr summary",
    }
    jira_data = {
        "BLOCK-456": "Jira summary",
    }

    result = build_block_response(zephyr_data, jira_data)

    assert result == {
        "BLOCK-T123": {
            "key": "BLOCK-T123",
            "summary": "Zephyr summary",
            "source": "zephyr",
            "url": f"{ZEPHYR_BLOCK_BASE_URL}BLOCK-T123",
        },
        "BLOCK-456": {
            "key": "BLOCK-456",
            "summary": "Jira summary",
            "source": "jira",
            "url": f"{JIRA_BLOCK_BASE_URL}BLOCK-456",
        },
    }


def test_build_block_response_zephyr_suffix_stripped():
    zephyr_data = {
        "BLOCK-T123_a": "Zephyr summary",
    }

    result = build_block_response(zephyr_data, {})

    assert result["BLOCK-T123_a"]["url"] == f"{ZEPHYR_BLOCK_BASE_URL}BLOCK-T123"


def test_build_block_response_empty_inputs():
    result = build_block_response({}, {})
    assert result == {}


def test_build_block_response_zephyr_only():
    zephyr_data = {
        "BLOCK-T123": "Zephyr summary",
    }

    result = build_block_response(zephyr_data, {})

    assert "BLOCK-T123" in result
    assert result["BLOCK-T123"]["source"] == "zephyr"


def test_build_block_response_jira_only():
    jira_data = {
        "BLOCK-456": "Jira summary",
    }

    result = build_block_response({}, jira_data)

    assert "BLOCK-456" in result
    assert result["BLOCK-456"]["source"] == "jira"
