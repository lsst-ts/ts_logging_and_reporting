import logging
from unittest.mock import Mock, patch

import pytest
import requests
from fastapi import HTTPException

from lsst.ts.logging_and_reporting.adapters.base_adapters import CachedAdapter
from lsst.ts.logging_and_reporting.adapters.base_clients import RestClient, SqlClient
from lsst.ts.logging_and_reporting.utils.auth import Server

SERVER = "https://client.test"


class Client(RestClient, CachedAdapter):
    """RestClient on the bare cache base: the transport, nothing else."""


class JiraClient(Client):
    auth_source = "jira"


class Sql(SqlClient, CachedAdapter):
    """SqlClient on the bare cache base."""


def mock_response(payload):
    response = Mock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def error_response(payload, status="500 Server Error", text="server error"):
    """Response whose raise_for_status raises an HTTPError carrying it."""
    response = Mock()
    response.text = text
    if isinstance(payload, Exception):
        response.json.side_effect = payload
    else:
        response.json.return_value = payload
    response.raise_for_status.side_effect = requests.HTTPError(status, response=response)
    return response


@pytest.fixture
def client(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return Client(fake_redis, server_url=SERVER)


@pytest.fixture
def sql_client(fake_redis, monkeypatch):
    monkeypatch.setenv("ACCESS_TOKEN", "test-token")
    return Sql(fake_redis, server_url=SERVER)


class TestServerResolution:
    def test_explicit_server_url_wins_over_the_environment(self, fake_redis, monkeypatch):
        monkeypatch.setenv("EXTERNAL_INSTANCE_URL", Server.summit)
        assert Client(fake_redis, server_url=SERVER).server == SERVER

    def test_falls_back_to_the_deployment_url(self, fake_redis, monkeypatch):
        monkeypatch.setenv("EXTERNAL_INSTANCE_URL", Server.summit)
        assert Client(fake_redis).server == Server.summit

    def test_unrecognised_deployment_url_raises(self, fake_redis, monkeypatch):
        monkeypatch.setenv("EXTERNAL_INSTANCE_URL", "https://not-a-deployment.test")
        with pytest.raises(ValueError):
            Client(fake_redis).server


class TestAuthentication:
    def test_token_is_resolved_per_request(self, client, monkeypatch):
        # Rotation must not need a restart: the token is read at call
        # time, not cached on the client.
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            client._get_json(f"{SERVER}/first")
            monkeypatch.setenv("ACCESS_TOKEN", "rotated-token")
            client._get_json(f"{SERVER}/second")
        sent = [call.kwargs["headers"]["Authorization"] for call in mock_get.call_args_list]
        assert sent == ["Bearer test-token", "Bearer rotated-token"]

    def test_auth_source_selects_the_token(self, fake_redis, monkeypatch):
        monkeypatch.setenv("ACCESS_TOKEN", "rsp-token")
        monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            JiraClient(fake_redis, server_url=SERVER)._get_json(f"{SERVER}/issues")
        assert mock_get.call_args.kwargs["headers"]["Authorization"] == "Bearer jira-token"

    def test_missing_token_fails_before_the_request(self, fake_redis, monkeypatch):
        monkeypatch.delenv("JIRA_API_TOKEN", raising=False)
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            with pytest.raises(HTTPException) as excinfo:
                JiraClient(fake_redis, server_url=SERVER)._get_json(f"{SERVER}/issues")
        # An unresolvable service-account token is a misconfigured
        # deployment, not a rejected caller.
        assert excinfo.value.status_code == 500
        mock_get.assert_not_called()

    def test_empty_token_is_rejected(self, fake_redis, monkeypatch):
        monkeypatch.setenv("JIRA_API_TOKEN", "")
        with pytest.raises(ValueError):
            JiraClient(fake_redis, server_url=SERVER)._request_headers()


class TestGetJson:
    def test_returns_the_decoded_body(self, client):
        with patch("requests.Session.get", Mock(return_value=mock_response({"id": 7}))):
            assert client._get_json(f"{SERVER}/thing") == {"id": 7}

    def test_sends_url_and_params(self, client):
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            client._get_json(f"{SERVER}/thing", params={"instrument": "latiss"})
        assert mock_get.call_args.args[0] == f"{SERVER}/thing"
        assert mock_get.call_args.kwargs["params"] == {"instrument": "latiss"}

    def test_drops_none_params_but_keeps_falsy_ones(self, client):
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            client._get_json(f"{SERVER}/thing", params={"offset": 0, "text": "", "flag": None})
        assert mock_get.call_args.kwargs["params"] == {"offset": 0, "text": ""}

    def test_applies_connect_and_read_timeouts(self, client):
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            client._get_json(f"{SERVER}/thing")
        assert mock_get.call_args.kwargs["timeout"] == (Client.CONNECT_TIMEOUT, Client.READ_TIMEOUT)

    def test_unreachable_server_raises(self, client):
        with patch("requests.Session.get", Mock(side_effect=requests.ConnectionError("refused"))):
            with pytest.raises(requests.ConnectionError):
                client._get_json(f"{SERVER}/thing")

    def test_error_status_raises(self, client):
        with patch("requests.Session.get", Mock(return_value=error_response({}, status="404 Not Found"))):
            with pytest.raises(requests.HTTPError):
                client._get_json(f"{SERVER}/thing")


class TestPostJson:
    def test_sends_the_body_and_returns_the_decoded_response(self, client):
        mock_post = Mock(return_value=mock_response({"ok": True}))
        with patch("requests.Session.post", mock_post):
            result = client._post_json(f"{SERVER}/thing", {"query": "SELECT 1"})
        assert result == {"ok": True}
        assert mock_post.call_args.args[0] == f"{SERVER}/thing"
        assert mock_post.call_args.kwargs["json"] == {"query": "SELECT 1"}
        assert mock_post.call_args.kwargs["timeout"] == (Client.CONNECT_TIMEOUT, Client.READ_TIMEOUT)

    def test_unreachable_server_raises(self, client):
        with patch("requests.Session.post", Mock(side_effect=requests.ConnectionError("refused"))):
            with pytest.raises(requests.ConnectionError):
                client._post_json(f"{SERVER}/thing", {})


def page(size, start=0):
    return [{"id": start + index} for index in range(size)]


class TestPaging:
    def test_short_first_page_ends_the_fetch(self, client):
        mock_get = Mock(return_value=mock_response(page(1)))
        with patch("requests.Session.get", mock_get):
            records = client._get_json_paged(f"{SERVER}/records", {}, page_limit=2)
        assert records == page(1)
        assert mock_get.call_count == 1

    def test_full_page_is_followed_by_another_request(self, client):
        mock_get = Mock(side_effect=[mock_response(page(2)), mock_response(page(1, start=2))])
        with patch("requests.Session.get", mock_get):
            records = client._get_json_paged(f"{SERVER}/records", {}, page_limit=2)
        assert records == page(3)
        offsets = [call.kwargs["params"]["offset"] for call in mock_get.call_args_list]
        assert offsets == [0, 2]

    def test_exact_multiple_needs_a_final_empty_page(self, client):
        # A full last page is indistinguishable from more to come, so
        # paging only stops once a short (here empty) page comes back.
        mock_get = Mock(side_effect=[mock_response(page(2)), mock_response([])])
        with patch("requests.Session.get", mock_get):
            records = client._get_json_paged(f"{SERVER}/records", {}, page_limit=2)
        assert records == page(2)
        assert mock_get.call_count == 2

    def test_caller_params_are_preserved_and_not_mutated(self, client):
        params = {"instrument": "latiss"}
        mock_get = Mock(return_value=mock_response([]))
        with patch("requests.Session.get", mock_get):
            client._get_json_paged(f"{SERVER}/records", params, page_limit=2)
        assert params == {"instrument": "latiss"}
        assert mock_get.call_args.kwargs["params"] == {"instrument": "latiss", "limit": 2, "offset": 0}

    def test_record_cap_truncates_and_stops_fetching(self, client, caplog):
        client.MAX_RECORDS = 3
        pages = [mock_response(page(2)), mock_response(page(2, start=2)), mock_response(page(2, start=4))]
        mock_get = Mock(side_effect=pages)
        with patch("requests.Session.get", mock_get):
            with caplog.at_level(logging.WARNING):
                records = client._get_json_paged(f"{SERVER}/records", {}, page_limit=2)
        assert len(records) == 4
        assert mock_get.call_count == 2
        assert "record cap" in caplog.text

    def test_failure_mid_paging_propagates(self, client):
        mock_get = Mock(side_effect=[mock_response(page(2)), requests.HTTPError("502 Bad Gateway")])
        with patch("requests.Session.get", mock_get):
            with pytest.raises(requests.HTTPError):
                client._get_json_paged(f"{SERVER}/records", {}, page_limit=2)


class TestSqlQuery:
    def test_posts_sql_to_the_consdb_query_endpoint(self, sql_client):
        payload = {"columns": ["exposure_id", "day_obs"], "data": [[1, 20250101]]}
        mock_post = Mock(return_value=mock_response(payload))
        with patch("requests.Session.post", mock_post):
            rows = sql_client._query("SELECT exposure_id, day_obs FROM exposure")
        assert rows == [{"exposure_id": 1, "day_obs": 20250101}]
        assert mock_post.call_args.args[0] == f"{SERVER}/consdb/query"
        assert mock_post.call_args.kwargs["json"] == {"query": "SELECT exposure_id, day_obs FROM exposure"}

    def test_empty_result_gives_no_rows(self, sql_client):
        payload = {"columns": ["exposure_id"], "data": []}
        with patch("requests.Session.post", Mock(return_value=mock_response(payload))):
            assert sql_client._query("SELECT exposure_id FROM exposure") == []

    def test_sql_error_is_logged_with_the_postgres_message_and_reraised(self, sql_client, caplog):
        response = error_response({"message": 'column "nope" does not exist'})
        with patch("requests.Session.post", Mock(return_value=response)):
            with caplog.at_level(logging.ERROR):
                with pytest.raises(requests.HTTPError):
                    sql_client._query("SELECT nope FROM exposure")
        assert 'column "nope" does not exist' in caplog.text
        assert "SELECT nope FROM exposure" in caplog.text

    def test_connection_failure_propagates(self, sql_client):
        with patch("requests.Session.post", Mock(side_effect=requests.ConnectionError("refused"))):
            with pytest.raises(requests.ConnectionError):
                sql_client._query("SELECT 1")


class TestSqlErrorMessage:
    def test_uses_the_message_field(self):
        response = Mock()
        response.json.return_value = {"message": "syntax error"}
        assert Sql._error_message(requests.HTTPError("500", response=response)) == "syntax error"

    def test_falls_back_to_the_body_without_a_message_field(self):
        response = Mock(text="plain body")
        response.json.return_value = {"detail": "no message key here"}
        assert Sql._error_message(requests.HTTPError("500", response=response)) == "plain body"

    def test_falls_back_to_the_body_when_not_json(self):
        response = Mock(text="<html>502 Bad Gateway</html>")
        response.json.side_effect = ValueError("no json")
        assert Sql._error_message(requests.HTTPError("502", response=response)) == (
            "<html>502 Bad Gateway</html>"
        )

    def test_falls_back_to_the_exception_without_a_response(self):
        assert Sql._error_message(requests.HTTPError("boom")) == "boom"


class TestRowShaping:
    def test_zips_columns_and_data_into_row_dicts(self, sql_client):
        result = {"columns": ["a", "b"], "data": [[1, 2], [3, 4]]}
        assert sql_client._rows_from_result(result) == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]

    def test_no_data_gives_no_rows(self, sql_client):
        assert sql_client._rows_from_result({"columns": ["a"], "data": []}) == []
