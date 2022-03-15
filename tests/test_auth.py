import pytest

from adc.auth import SASLAuth, SASLMethod


@pytest.mark.parametrize('auth,expected_config', [
    [
        SASLAuth(
            'test', 'test-pass',
            token_endpoint='https://example.com/oauth2/token'
        ),
        {
            'sasl.mechanism': 'OAUTHBEARER',
            'sasl.oauthbearer.client.id': 'test',
            'sasl.oauthbearer.client.secret': 'test-pass',
            'sasl.oauthbearer.method': 'oidc',
            'sasl.oauthbearer.token.endpoint.url': 'https://example.com/oauth2/token',
            'security.protocol': 'SASL_SSL'
        }
    ],
    [
        SASLAuth(
            'test', 'test-pass',
        ),
        {
            'sasl.mechanism': 'PLAIN',
            'sasl.username': 'test',
            'sasl.password': 'test-pass',
            'security.protocol': 'SASL_SSL'
        }
    ]
])
def test_auth(auth, expected_config):
    # Check that the key/value pairs in expected_config are a subset
    # of the key/value pairs in auth._config.
    assert expected_config.items() <= auth._config.items()
