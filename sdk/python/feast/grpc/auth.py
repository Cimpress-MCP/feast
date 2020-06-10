import grpc
from google.auth.exceptions import DefaultCredentialsError

from feast.config import Config
from feast.constants import AuthProvider, \
    CONFIG_CORE_ENABLE_AUTH_TOKEN_KEY, \
    CONFIG_OAUTH_CLIENT_ID_KEY, \
    CONFIG_OAUTH_CLIENT_SECRET_KEY, \
    CONFIG_OAUTH_AUDIENCE_KEY, \
    CONFIG_OAUTH_TOKEN_REQUEST_URL_KEY, \
    CONFIG_CORE_AUTH_PROVIDER, \
    CONFIG_OAUTH_GRANT_TYPE_KEY, \
    CONFIG_OAUTH_CONTENT_TYPE_KEY


def get_auth_metadata_plugin(config: Config):
    """
    Get an Authentication Metadata Plugin. This plugin is used in gRPC to
    sign requests. Please see the following URL for more details
    https://grpc.github.io/grpc/python/_modules/grpc.html#AuthMetadataPlugin

    New plugins can be added to this function. For the time being we only
    support Google Open ID authentication.

    Returns: Returns an implementation of grpc.AuthMetadataPlugin

    Args:
        config: Feast Configuration object
    """
    if config.get(CONFIG_CORE_AUTH_PROVIDER) == AuthProvider.GOOGLE:
        return GoogleOpenIDAuthMetadataPlugin(config)
    elif config.get(CONFIG_CORE_AUTH_PROVIDER) == AuthProvider.OAUTH:
        return OAuthMetadataPlugin(config)
    else:
        raise RuntimeError(
            "Could not determine OAuth provider."
            "Must be an enum - AuthProvider.GOOGLE or AuthProvider.OAUTH"
        )


class OAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """A `gRPC AuthMetadataPlugin`_ that inserts the credentials into each
    request.

    .. _gRPC AuthMetadataPlugin:
        http://www.grpc.io/grpc/python/grpc.html#grpc.AuthMetadataPlugin
    """

    def __init__(self, config: Config):
        """
        Initializes an OAuthMetadataPlugin, used to sign gRPC requests
        Args:
            config: Feast Configuration object
        """
        super(OAuthMetadataPlugin, self).__init__()

        self._static_token = None
        self._token = None

        # If provided, set a static token
        if config.exists(CONFIG_CORE_ENABLE_AUTH_TOKEN_KEY):
            self._static_token = config.get(CONFIG_CORE_ENABLE_AUTH_TOKEN_KEY)

        self._refresh_token(config)

    def get_signed_meta(self):
        """ Creates a signed authorization metadata token."""
        return (("authorization", "Bearer {}".format(self._token)),)

    def _refresh_token(self, config: Config):
        """ Refreshes OAuth token and persists it in memory """

        # Use static token if available
        if self._static_token:
            self._token = self._static_token
            return

        import json
        import requests

        try:
            headers_token = {
                'content-type': config.get(CONFIG_OAUTH_CONTENT_TYPE_KEY),
            }
            data_token = {
                "grant_type": config.get(CONFIG_OAUTH_GRANT_TYPE_KEY),
                "client_id": config.get(CONFIG_OAUTH_CLIENT_ID_KEY),
                "client_secret": config.get(CONFIG_OAUTH_CLIENT_SECRET_KEY),
                "audience": config.get(CONFIG_OAUTH_AUDIENCE_KEY)
            }
            data_token = json.dumps(data_token)
            response_token = requests.post(config.get(CONFIG_OAUTH_TOKEN_REQUEST_URL_KEY),
                                           headers=headers_token, data=data_token)
            return response_token.json().get('access_token')

        except:
            raise RuntimeError(
                "Could not determine OAuth token. Please ensure that the "
                "necessary parameters are passed to the client - "
                "Content type, Grant type, Client ID, Client secret, Audience, Token request URL."
            )

    def set_static_token(self, token):
        """
        Define a static token to return

        Args:
            token: String token
        """
        self._static_token = token


class GoogleOpenIDAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """A `gRPC AuthMetadataPlugin`_ that inserts the credentials into each
    request.

    .. _gRPC AuthMetadataPlugin:
        http://www.grpc.io/grpc/python/grpc.html#grpc.AuthMetadataPlugin
    """

    def __init__(self, config: Config):
        """
        Initializes a GoogleOpenIDAuthMetadataPlugin, used to sign gRPC requests
        Args:
            config: Feast Configuration object
        """
        super(GoogleOpenIDAuthMetadataPlugin, self).__init__()
        from google.auth.transport import requests

        self._static_token = None
        self._token = None

        # If provided, set a static token
        if config.exists(CONFIG_CORE_ENABLE_AUTH_TOKEN_KEY):
            self._static_token = config.get(CONFIG_CORE_ENABLE_AUTH_TOKEN_KEY)

        self._request = requests.Request()
        self._refresh_token()

    def get_signed_meta(self):
        """ Creates a signed authorization metadata token."""
        return (("authorization", "Bearer {}".format(self._token)),)

    def _refresh_token(self):
        """ Refreshes Google ID token and persists it in memory """

        # Use static token if available
        if self._static_token:
            self._token = self._static_token
            return

        # Try to find ID Token from Gcloud SDK
        from google.auth import jwt
        import subprocess

        cli_output = subprocess.run(
            ["gcloud", "auth", "print-identity-token"], stdout=subprocess.PIPE
        )
        token = cli_output.stdout.decode("utf-8").strip()
        try:
            jwt.decode(token, verify=False)  # Ensure the token is valid
            self._token = token
            return
        except ValueError:
            pass  # GCloud command not successful

        # Try to use Google Auth library to find ID Token
        from google import auth as google_auth

        try:
            credentials, _ = google_auth.default(["openid", "email"])
            credentials.refresh(self._request)
            if hasattr(credentials, "id_token"):
                self._token = credentials.id_token
                return
        except DefaultCredentialsError:
            pass  # Could not determine credentials, skip

        # Raise exception otherwise
        raise RuntimeError(
            "Could not determine Google ID token. Please ensure that the "
            "Google Cloud SDK is installed or that a service account can be "
            "found using the GOOGLE_APPLICATION_CREDENTIALS environmental "
            "variable."
        )

    def set_static_token(self, token):
        """
        Define a static token to return

        Args:
            token: String token
        """
        self._static_token = token


def __call__(self, context, callback):
    """Passes authorization metadata into the given callback.

    Args:
        context (grpc.AuthMetadataContext): The RPC context.
        callback (grpc.AuthMetadataPluginCallback): The callback that will
            be invoked to pass in the authorization metadata.
    """
    callback(self.get_signed_meta(), None)
