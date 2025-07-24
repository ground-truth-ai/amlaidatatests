"""Tests for the connection factory."""

import unittest.mock

import pytest
from ibis import BaseBackend
from omegaconf import OmegaConf

from amlaidatatests import __version__
from amlaidatatests.config import DATATEST_STRUCTURED_CONFIG, ConfigSingleton
from amlaidatatests.connection import connection_factory


@unittest.mock.patch("ibis.bigquery.connect")
@unittest.mock.patch("google.cloud.bigquery.Client")
@unittest.mock.patch("google.auth.default")
def test_bigquery_user_agent(
    mock_auth, mock_bq_client, mock_ibis_connect, test_connection
):
    """Verify that the BigQuery client is created with the correct user agent."""
    # Arrange
    mock_auth.return_value = (None, "test-project")
    conf = ConfigSingleton.get()
    new_conf = OmegaConf.merge(
        conf, {"connection_string": "bigquery://my-project/my_dataset"}
    )
    ConfigSingleton().set_config(new_conf)

    # Act
    connection_factory()

    # Assert
    mock_bq_client.assert_called_once()
    _, kwargs = mock_bq_client.call_args
    client_info = kwargs.get("client_info")
    assert client_info.user_agent == f"gtai-amlaidatatests/{__version__}"
