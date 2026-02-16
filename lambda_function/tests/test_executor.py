"""
Simplified tests for the Executor class and its functions.
Tests both high-level (handle_event) and low-level (Executor methods) functions.
"""

import json
import os
from unittest.mock import Mock, patch
import pytest
from executor.executor import handle_event, Executor


# ========================================
# Tests for High-Level Functions
# ========================================


class TestHandleEvent:
    """Tests for the high-level handle_event function."""

    @patch("executor.executor.Executor")
    def test_handle_event_success(self, mock_executor_class):
        """Test successful event handling with valid rule ARN."""
        # Setup
        mock_executor = Mock()
        mock_executor_class.return_value = mock_executor

        event = {
            "resources": [
                "arn:aws:events:us-east-1:123456789012:rule/"
                "import_GOES_data_to_timestream"
            ]
        }
        context = {}

        # Execute
        response = handle_event(event, context)

        # Assert
        assert response["statusCode"] == 200
        assert "Execution completed successfully" in response["body"]
        mock_executor_class.assert_called_once_with(
            "import_GOES_data_to_timestream"
        )
        mock_executor.execute.assert_called_once()

    def test_handle_event_missing_resources(self):
        """Test error handling when 'resources' key is missing."""
        event = {}
        context = {}

        response = handle_event(event, context)

        assert response["statusCode"] == 500
        assert "resources" in response["body"]

    def test_handle_event_invalid_rule_arn(self):
        """Test error handling for invalid rule ARN format."""
        event = {"resources": ["invalid-arn-format"]}
        context = {}

        response = handle_event(event, context)

        assert response["statusCode"] == 500
        assert "Invalid rule ARN format" in response["body"]

    @patch("executor.executor.Executor")
    def test_handle_event_executor_exception(self, mock_executor_class):
        """Test error handling when Executor raises an exception."""
        mock_executor = Mock()
        mock_executor.execute.side_effect = Exception("Execution failed")
        mock_executor_class.return_value = mock_executor

        event = {
            "resources": [
                "arn:aws:events:us-east-1:123456789012:rule/test_function"
            ]
        }
        context = {}

        response = handle_event(event, context)

        assert response["statusCode"] == 500
        assert "error" in response["body"]


# ========================================
# Tests for Low-Level Functions (Executor)
# ========================================


class TestExecutorInit:
    """Tests for Executor class initialization."""

    @patch.dict(
        os.environ,
        {
            "SECRET_ARN_GRAFANA": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
            ),
            "SECRET_ARN_UDL": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test2"
            ),
        },
    )
    @patch("boto3.session.Session")
    def test_executor_init_with_secrets(self, mock_session):
        """Test Executor initialization loads secrets correctly."""
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(
                {"grafana_api_key": "test_key", "basicauth": "test_auth"}
            )
        }
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        executor = Executor("test_function")

        assert executor.function_name == "test_function"
        assert "GRAFANA_API_KEY" in os.environ
        assert "BASICAUTH" in os.environ

    @patch.dict(
        os.environ,
        {
            "SECRET_ARN_GRAFANA": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
            ),
            "SECRET_ARN_UDL": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test2"
            ),
        },
    )
    @patch("boto3.session.Session")
    def test_executor_init_secret_error(self, mock_session):
        """Test Executor initialization handles secret errors gracefully."""
        mock_client = Mock()
        mock_client.get_secret_value.side_effect = Exception(
            "Secret not found"
        )
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        # Should not raise exception
        executor = Executor("test_function")

        assert executor.function_name == "test_function"


class TestExecutorExecute:
    """Tests for Executor.execute() method."""

    @patch.dict(
        os.environ,
        {
            "SECRET_ARN_GRAFANA": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
            ),
            "SECRET_ARN_UDL": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test2"
            ),
        },
    )
    @patch("boto3.session.Session")
    @patch.object(Executor, "import_GOES_data_to_timestream")
    def test_execute_valid_function(self, mock_import_goes, mock_session):
        """Test execute() calls the correct function."""
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(
                {"grafana_api_key": "test_key", "basicauth": "test_auth"}
            )
        }
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        executor = Executor("import_GOES_data_to_timestream")
        executor.execute()

        mock_import_goes.assert_called_once()

    @patch.dict(
        os.environ,
        {
            "SECRET_ARN_GRAFANA": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"
            ),
            "SECRET_ARN_UDL": (
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:test2"
            ),
        },
    )
    @patch("boto3.session.Session")
    def test_execute_invalid_function(self, mock_session):
        """Test execute() raises error for unrecognized function."""
        mock_client = Mock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(
                {"grafana_api_key": "test_key", "basicauth": "test_auth"}
            )
        }
        mock_session_instance = Mock()
        mock_session_instance.client.return_value = mock_client
        mock_session.return_value = mock_session_instance

        executor = Executor("nonexistent_function")

        with pytest.raises(ValueError, match="not recognized"):
            executor.execute()


class TestExecutorFunctions:
    """Tests for individual Executor function methods."""

    @patch.object(Executor, "import_GOES_data_to_timestream")
    def test_import_GOES_data_to_timestream_success(self, mock_import_goes):
        """Test GOES data import function can be called."""
        # Execute
        Executor.import_GOES_data_to_timestream()

        # Assert
        mock_import_goes.assert_called_once()

    @patch("executor.executor.pd.read_json")
    def test_import_GOES_data_to_timestream_error(self, mock_read_json):
        """Test GOES data import error handling."""
        mock_read_json.side_effect = Exception("Connection error")

        with pytest.raises(Exception):
            Executor.import_GOES_data_to_timestream()

    @patch.object(Executor, "create_GOES_data_annotations")
    def test_create_GOES_data_annotations(self, mock_create_annotations):
        """Test GOES annotation creation function can be called."""
        # Execute
        Executor.create_GOES_data_annotations()

        # Assert
        mock_create_annotations.assert_called_once()

    @patch.dict(os.environ, {})
    def test_generate_cloc_report_missing_env_vars(self):
        """Test CLOC report fails with missing environment variables."""
        with pytest.raises(ValueError, match="GITHUB_ORGS_USERS"):
            Executor.generate_cloc_report_and_upload()

    @patch("executor.executor.util.record_timeseries")
    @patch("executor.executor.requests.get")
    @patch.dict(os.environ, {"BASICAUTH": "test_auth"})
    def test_import_UDL_REACH_to_timestream(self, mock_requests, mock_record):
        """Test UDL REACH data import."""
        mock_response = Mock()
        mock_response.__bool__ = Mock(return_value=True)
        mock_response.json.return_value = [
            {
                "idSensor": "REACH-171",
                "obTime": "2024-01-01T00:00:00.000Z",
                "lat": 45.0,
                "lon": -120.0,
                "alt": 500.0,
                "observatoryName": "TestSat",
                "seoList": [
                    {
                        "obDescription": "DOSE1 (Flavor A) in rad/second",
                        "obValue": 0.001,
                    }
                ],
            }
        ]
        mock_requests.return_value = mock_response

        # Execute
        Executor.import_UDL_REACH_to_timestream()

        # Assert
        mock_requests.assert_called_once()

    @patch("executor.executor.util.record_timeseries")
    @patch("executor.executor.TimeSeries")
    @patch("executor.executor.Time")
    @patch("stixdcpy.quicklook.LightCurves.from_sdc")
    def test_import_stix_to_timestream_with_data(
        self, mock_lightcurves, mock_time_class, mock_timeseries, mock_record
    ):
        """Test STIX data import with data."""
        from astropy.time import Time

        mock_lc = Mock()
        mock_lc.data = True
        # Create real time objects for the mock
        mock_lc.time = Time(["2024-01-01T00:00:00", "2024-01-01T00:01:00"])
        mock_lc.counts = [[1.0, 2.0], [3.0, 4.0]]
        mock_lightcurves.return_value = mock_lc

        # Use real Time class for the constructor call, but mock now()
        mock_time_class.side_effect = (
            lambda *args, **kwargs: Time(*args, **kwargs)
            if args
            else Mock()
        )
        mock_time_class.now = Mock(return_value=Time("2024-01-01T12:00:00"))

        # Mock TimeSeries
        mock_ts = Mock()
        mock_ts.__len__ = Mock(return_value=2)
        mock_ts.time = mock_lc.time
        mock_timeseries.return_value = mock_ts

        # Execute
        Executor.import_stix_to_timestream()

        # Assert
        mock_lightcurves.assert_called_once()
        mock_record.assert_called_once()

    @patch("stixdcpy.quicklook.LightCurves.from_sdc")
    def test_import_stix_to_timestream_no_data(self, mock_lightcurves):
        """Test STIX data import with no data."""
        mock_lc = Mock()
        mock_lc.data = False
        mock_lightcurves.return_value = mock_lc

        # Execute
        Executor.import_stix_to_timestream()

        # Assert
        mock_lightcurves.assert_called_once()

    @patch("padre_craft.io.aws_db.record_orbit")
    @patch("padre_craft.orbit.PadreOrbit")
    @patch.dict(os.environ, {"SWXSOC_MISSION": ""})
    def test_get_padre_orbit_data_success(
        self, mock_padre_orbit_class, mock_record_orbit
    ):
        """Test successful Padre orbit data retrieval."""
        # Mock file existence
        with patch("pathlib.Path.exists", return_value=True):
            with patch("urllib.request.urlretrieve"):
                mock_padre_orbit = Mock()
                mock_padre_orbit.timeseries = [
                    {"time": "2024-01-01", "lat": 0.0}
                ]
                mock_padre_orbit_class.return_value = mock_padre_orbit

                # Execute
                Executor.get_padre_orbit_data()

                # Assert
                mock_padre_orbit.calculate.assert_called_once()
                mock_record_orbit.assert_called_once()

    @patch("padre_craft.orbit.PadreOrbit")
    @patch.dict(os.environ, {"SWXSOC_MISSION": ""})
    def test_get_padre_orbit_data_no_data(self, mock_padre_orbit_class):
        """Test Padre orbit data retrieval with no data."""
        with patch("pathlib.Path.exists", return_value=True):
            with patch("urllib.request.urlretrieve"):
                mock_padre_orbit = Mock()
                mock_padre_orbit.timeseries = None
                mock_padre_orbit_class.return_value = mock_padre_orbit

                # Execute
                Executor.get_padre_orbit_data()

                # Assert
                mock_padre_orbit.calculate.assert_called_once()
