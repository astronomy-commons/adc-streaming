#!/usr/bin/env python

import certifi


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
    method : `str`, optional
        The SASL method to authenticate, default = PLAIN.
        Options are [PLAIN, SCRAM-SHA-256, SCRAM-SHA-512].
    ssl_ca_location : `str`, optional
        If using SSL via a self-signed cert, a path/location
        to the certificate.

    """

    _methods = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]

    def __init__(self, user, password, ssl=True, method="PLAIN", **kwargs):
        self._method = method.upper()
        assert self._method in self._methods

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
        self._config["sasl.mechanism"] = self._method
        self._config["sasl.username"] = user
        self._config["sasl.password"] = password

    def __call__(self):
        return self._config
