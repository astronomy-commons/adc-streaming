#!/usr/bin/env python

from enum import Enum

import certifi


class SASLMethod(Enum):
    """SASL method to use for authentication.
    """

    PLAIN = 1
    SCRAM_SHA_256 = 2
    SCRAM_SHA_512 = 3
    OAUTHBEARER = 4

    def __str__(self):
        return self.name.replace("_", "-")


class SASLAuth(object):
    """Attach SASL-based authentication to a client.

    Returns client-based auth options when called.

    Parameters
    ----------
    user : `str`
        Username to authenticate with.
    password : `str`
        Password to authenticate with.
    ssl : `bool`, optional
        Whether to enable SSL (enabled by default).
    method : `SASLMethod`, optional
        The SASL method to authenticate. The default is SASLMethod.OAUTHBEARER
        if token_endpoint is provided, or SASLMethod.PLAIN otherwise.
        See valid SASL methods in SASLMethod.
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.
    token_endpoint : `str`, optional
        The OpenID Connect token endpoint URL.
        Required for OAUTHBEARER / OpenID Connect, otherwise ignored.

    """

    def __init__(self, user, password, ssl=True, method=None, token_endpoint=None, **kwargs):
        if method is None:
            if token_endpoint is None:
                method = SASLMethod.PLAIN
            else:
                method = SASLMethod.OAUTHBEARER
        self._method = method

        # set up SSL options
        if ssl:
            if "ssl_ca_location" in kwargs:
                ssl_cert = kwargs["ssl_ca_location"]
            else:
                ssl_cert = certifi.where()

            self._config = {
                "security.protocol": "SASL_SSL",
                "ssl.ca.location": ssl_cert,
            }
        else:
            self._config = {"security.protocol": "SASL_PLAINTEXT"}

        # set up SASL options
        self._config["sasl.mechanism"] = str(self._method)
        if token_endpoint:
            self._config["sasl.oauthbearer.client.id"] = user
            self._config["sasl.oauthbearer.client.secret"] = password
            self._config["sasl.oauthbearer.method"] = "oidc"
            self._config["sasl.oauthbearer.token.endpoint.url"] = token_endpoint
        else:
            self._config["sasl.username"] = user
            self._config["sasl.password"] = password

    def __call__(self):
        return self._config
