#!/usr/bin/env python

import base64
from collections.abc import Mapping
from enum import Enum
import json
import logging
import subprocess

import certifi

logger = logging.getLogger("adc-streaming.auth")


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
    ssl_endpoint_identification_algorithm : `str`, optional
        If using SSL, the algorithm used to verify that certificate is valid for the endpoint.
    token_endpoint : `str`, optional
        The OpenID Connect token endpoint URL.
        Required for OAUTHBEARER / OpenID Connect, otherwise ignored.

    """

    def __init__(self, user, password, ssl=True, method=None, token_endpoint=None, **kwargs):
        if method is None:
            if token_endpoint is not None or "token_command" in kwargs:
                method = SASLMethod.OAUTHBEARER
            else:
                method = SASLMethod.PLAIN

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
                "https.ca.location": ssl_cert,
            }
            if "ssl_endpoint_identification_algorithm" in kwargs:
                self._config["ssl.endpoint.identification.algorithm"] = \
                    kwargs["ssl_endpoint_identification_algorithm"]
        else:
            self._config = {"security.protocol": "SASL_PLAINTEXT"}

        # set up SASL options
        self._config["sasl.mechanism"] = str(self._method)
        if method == SASLMethod.OAUTHBEARER:
            if token_endpoint is not None:
                self._config["sasl.oauthbearer.client.id"] = user
                self._config["sasl.oauthbearer.client.secret"] = password
                self._config["sasl.oauthbearer.method"] = "oidc"
                self._config["sasl.oauthbearer.token.endpoint.url"] = token_endpoint
            elif "token_command" in kwargs:
                token_command = kwargs["token_command"]
                self._config["sasl.oauthbearer.method"] = "default"
                self._config["oauth_cb"] = lambda c: SASLAuth.external_token_callback(token_command)
        else:
            self._config["sasl.username"] = user
            self._config["sasl.password"] = password

    @staticmethod
    def external_token_callback(command):
        try:
            presult = subprocess.run(command, shell=True, capture_output=True)
            if presult.returncode != 0:
                raise RuntimeError("Token callback command failed: " + presult.stderr)
            rawdata = presult.stdout
            # need to parse the resulting JWT enough to extract the expiration time
            # we do no other validation, since downstream systems should be responsible
            # for that already
            sections = rawdata.split(b'.')
            if len(sections) != 3:
                raise RuntimeError("Token callback output does not appear to be a valid JWT")
            try:
                # The JWT spec mandates that base 64 padding be omitted, but
                # base64.urlsafe_b64decode requires it, so we must put it back.
                m = len(sections[1]) % 4
                if m == 2:
                    sections[1] += b"=="
                elif m == 3:
                    sections[1] += b"="
                elif m == 1:
                    raise RuntimeError("Token callback output is not valid base64 data")
                claims = json.loads(base64.urlsafe_b64decode(sections[1]).decode("utf-8"))
            except UnicodeDecodeError:
                raise RuntimeError("Token callback output is not valid UTF-8 after base64 decoding")
            except json.JSONDecodeError:
                raise RuntimeError("Token callback output is not valid JSON after base64 and "
                                   "UTF-8 decoding")
            if not isinstance(claims, Mapping):
                raise RuntimeError("Token callback output does not contain valid claims")
            if "sub" not in claims:
                raise RuntimeError("Token callback output does not contain an subject claim")
            if "exp" not in claims:
                raise RuntimeError("Token callback output does not contain an expiration claim")
            exp_value = claims["exp"]
            if not isinstance(exp_value, int) and not isinstance(exp_value, float):
                raise RuntimeError("Token expiration value is not a number")
            exp_value = float(exp_value)
            try:
                return (rawdata.decode("utf-8").strip(), exp_value, claims["sub"], {})
            except UnicodeDecodeError:
                raise RuntimeError("Token callback output is not valid UTF-8 data")
        except Exception as ex:
            logger.error(f"Token callback error: {ex}")
            raise

    def __call__(self):
        return self._config
