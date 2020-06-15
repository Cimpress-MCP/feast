from feast.grpc.auth import get_auth_metadata_plugin, GoogleOpenIDAuthMetadataPlugin, OAuthMetadataPlugin
from pytest import fixture, raises
from unittest.mock import patch, call
from feast.config import Config
from http import HTTPStatus
import json

AUDIENCE = "https://testaudience.io/"

AUTH_URL = "https://test.auth.com/v2/token"

HEADERS = {'content-type': "application/json"}

DATA = json.dumps({
    "grant_type": "client_credentials",
    "client_id": "fakeID",
    "client_secret": "fakeSecret",
    "audience": AUDIENCE
})


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


class GoogleSubprocessResponse:
    def __init__(self, output):
        self.stdout = output["stdout"]


class GoogleDecodeResponse:
    def __init__(self, stdout):
        self.stdout = stdout


class GoogleDefaultResponse:
    def __init__(self, id_token):
        self.id_token = id_token

    def refresh(self, request):
        pass


class GoogleDefaultErrorResponse:
    def __init__(self, id_token):
        self.wrong_attribute = id_token

    def refresh(self, request):
        pass


@fixture
def config_oauth():
    config_dict = {"core_url": "localhost:50051",
                   "core_enable_auth": True,
                   "core_auth_provider": "OAuth",
                   "oauth_grant_type": "client_credentials",
                   "oauth_client_id": "fakeID",
                   "oauth_client_secret": "fakeSecret",
                   "oauth_audience": AUDIENCE,
                   "oauth_token_request_url": AUTH_URL}
    return Config(config_dict)


@fixture
def config_google():
    config_dict = {"core_url": "localhost:50051",
                   "core_enable_auth": True,
                   "core_auth_provider": "Google",
                   "oauth_grant_type": "client_credentials",
                   "oauth_client_id": "fakeID",
                   "oauth_client_secret": "fakeSecret",
                   "oauth_audience": AUDIENCE,
                   "oauth_token_request_url": AUTH_URL}
    return Config(config_dict)


@fixture
def config_with_missing_variable():
    config_dict = {"core_url": "localhost:50051",
                   "core_enable_auth": True,
                   "core_auth_provider": "OAuth",
                   "oauth_grant_type": "client_credentials",
                   "oauth_client_id": "fakeID",
                   "oauth_client_secret": "fakeSecret",
                   "oauth_token_request_url": AUTH_URL}
    return Config(config_dict)


@patch("requests.post", return_value=MockResponse({"access_token": "mock_token"}, HTTPStatus.OK))
def test_get_auth_metadata_plugin_oauth_should_pass(post, config_oauth):
    auth_metadata_plugin = get_auth_metadata_plugin(config_oauth)
    assert isinstance(auth_metadata_plugin, OAuthMetadataPlugin)
    assert post.call_count == 1
    assert post.call_args == call(AUTH_URL, headers=HEADERS, data=DATA)


@patch("requests.post", return_value=MockResponse({"access_token": "mock_token"}, HTTPStatus.UNAUTHORIZED))
def test_get_auth_metadata_plugin_oauth_should_raise_when_response_is_not_200(post, config_oauth):
    with raises(RuntimeError):
        get_auth_metadata_plugin(config_oauth)
        assert post.call_count == 1
        assert post.call_args == call(AUTH_URL, headers=HEADERS, data=DATA)


def test_get_auth_metadata_plugin_oauth_should_raise_when_config_is_incorrect(config_with_missing_variable):
    with raises(RuntimeError):
        get_auth_metadata_plugin(config_with_missing_variable)


@patch("google.auth.jwt.decode", return_value=GoogleDecodeResponse("jwt_token"))
@patch("subprocess.run", return_value=GoogleSubprocessResponse({"stdout": "std_output".encode("utf-8")}))
def test_get_auth_metadata_plugin_google_should_pass_with_token_from_gcloud_sdk(subprocess, jwt, config_google):
    auth_metadata_plugin = get_auth_metadata_plugin(config_google)
    assert isinstance(auth_metadata_plugin, GoogleOpenIDAuthMetadataPlugin)


@patch("google.auth.default", return_value=[GoogleDefaultResponse("fake_token"), GoogleDefaultResponse("project_id")])
@patch("subprocess.run", return_value=GoogleSubprocessResponse({"stdout": "std_output".encode("utf-8")}))
def test_get_auth_metadata_plugin_google_should_pass_with_token_from_google_auth_lib(subprocess, default, config_google):
    auth_metadata_plugin = get_auth_metadata_plugin(config_google)
    assert isinstance(auth_metadata_plugin, GoogleOpenIDAuthMetadataPlugin)


@patch("google.auth.default", return_value=[GoogleDefaultErrorResponse("fake_token"), GoogleDefaultErrorResponse("project_id")])
@patch("subprocess.run", return_value=GoogleSubprocessResponse({"stdout": "std_output".encode("utf-8")}))
def test_get_auth_metadata_plugin_google_should_raise_when_token_validation_fails(subprocess, default, config_google):
    with raises(RuntimeError):
        get_auth_metadata_plugin(config_google)
