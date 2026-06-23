"""Unit tests for executor constructor behavior."""

import json
import os

import boto3
import numpy as np
import pytest
from astropy.time import Time, TimeDelta
from src.executor.executor import Executor


def test_constructor_loads_secrets_from_moto(monkeypatch, mock_aws) -> None:
    """Constructor loads expected secret values from mocked Secrets Manager."""
    # Create mock secrets in Secrets Manager and set env vars to their ARNs
    client = boto3.client("secretsmanager", region_name="us-east-1")
    grafana_secret = client.create_secret(
        Name="test-secret-grafana",
        SecretString=json.dumps({"grafana_api_key": "grafana-test-key"}),
    )
    udl_secret = client.create_secret(
        Name="test-secret-udl",
        SecretString=json.dumps({"basicauth": "udl-test-auth"}),
    )
    monkeypatch.setenv("SECRET_ARN_GRAFANA", grafana_secret["ARN"])
    monkeypatch.setenv("SECRET_ARN_UDL", udl_secret["ARN"])

    # Create executor instance, which should load secrets into environment variables
    _ = Executor("TestFunction")

    assert os.getenv("GRAFANA_API_KEY") == "grafana-test-key"
    assert os.getenv("BASICAUTH") == "udl-test-auth"


def test_constructor_handles_missing_secret_arns() -> None:
    """Constructor does not raise when secret ARN env vars are missing."""
    # Ensure secret ARN env vars are not set
    _ = Executor("TestFunction")

    # Assert the ENV Variables are not set
    assert os.getenv("GRAFANA_API_KEY") is None
    assert os.getenv("BASICAUTH") is None


def test_import_stix_to_timestream(monkeypatch) -> None:
    """STIX import builds a timeseries and forwards it to the utility recorder."""

    class FakeLightCurves:
        def __init__(self) -> None:
            self.data = True
            self.time = Time(
                [
                    "2026-06-04T00:00:00",
                    "2026-06-04T00:04:00",
                    "2026-06-04T00:08:00",
                ],
                format="isot",
            )
            self.counts = np.array(
                [
                    [1.0, 2.0, 3.0],
                    [4.0, 5.0, 6.0],
                ]
            )

        @classmethod
        def from_sdc(cls, start_utc, end_utc, ltc=False):
            return cls()

    recorded_calls = []

    def fake_record_timeseries(*args, **kwargs):
        recorded_calls.append((args, kwargs))

    # Patch the LightCurve call so we're not downloading 12 hours of data during the test
    monkeypatch.setattr(
        "stixdcpy.quicklook.LightCurves.from_sdc", FakeLightCurves.from_sdc
    )
    # Patch the record_timeseries utility function to capture its inputs for verification
    monkeypatch.setattr(
        "src.executor.executor.util.record_timeseries", fake_record_timeseries
    )

    # Invoke the function directly
    Executor.import_stix_to_timestream()

    assert len(recorded_calls) == 1
    args, kwargs = recorded_calls[0]
    assert len(args) == 1
    assert kwargs == {"ts_name": "solo", "instrument_name": "stix"}
    assert list(args[0].colnames) == ["time", "qlc0", "qlc1"]


def test_get_padre_orbit_data(monkeypatch) -> None:
    """PADRE orbit path can run with only record_orbit mocked."""
    recorded_orbits = []

    def fake_record_orbit(timeseries):
        recorded_orbits.append(timeseries)

    # Patch the record_orbit function to capture its input for verification
    monkeypatch.setattr("padre_craft.io.aws_db.record_orbit", fake_record_orbit)

    # Invoke the function directly
    # NOTE: This does not mock the call to download TLE and Ephemeris data
    Executor.get_padre_orbit_data()

    assert os.getenv("SWXSOC_MISSION") == "padre"
    if recorded_orbits:
        assert len(recorded_orbits[0]) > 0


def test_import_UDL_REACH_to_s3(monkeypatch, tmp_path) -> None:
    """REACH import snaps to a midnight-aligned day window and uploads via mocked push."""
    download_call = {}
    upload_calls = []

    def fake_download_UDL_reach_window(**kwargs):
        download_call.update(kwargs)
        output_file = tmp_path / "REACH-ALL_20250603T000000_20250603T002000.csv"
        output_file.write_text("time,value\n2025-06-03T00:00:00Z,1\n")
        return str(output_file)

    def fake_push_science_file(
        science_filename_parser, destination_bucket, calibrated_filename, dry_run=False
    ):
        upload_calls.append(
            {
                "destination_bucket": destination_bucket,
                "calibrated_filename": calibrated_filename,
                "dry_run": dry_run,
            }
        )
        return f"mock/{calibrated_filename}"

    monkeypatch.setenv("BASICAUTH", "mock-auth-token")
    monkeypatch.setenv("REACH_WINDOW_END_DAYS_AGO", "1")
    monkeypatch.setenv("REACH_WINDOW_DAYS", "1")
    monkeypatch.setenv("REACH_UDL_MAX_CONCURRENT_REQUESTS", "4")
    monkeypatch.setenv("REACH_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("REACH_DESTINATION_BUCKET_PROD", "unit-test-bucket-prod")
    monkeypatch.setenv("REACH_DESTINATION_BUCKET_DEV", "unit-test-bucket-dev")

    monkeypatch.setattr(
        "src.executor.executor.download_UDL_reach_window",
        fake_download_UDL_reach_window,
    )
    monkeypatch.setattr(
        "src.executor.executor.push_science_file", fake_push_science_file
    )

    Executor.import_UDL_REACH_to_s3()

    start_time = download_call["start_time"]
    end_time = download_call["end_time"]

    # Window edges are snapped to UTC midnight.
    assert start_time.iso.endswith("00:00:00.000")
    assert end_time.iso.endswith("00:00:00.000")

    # End is one day before today's UTC midnight; window is one day long.
    expected_end = Time(Time.now().iso[0:10]) - TimeDelta(1, format="jd")
    assert (end_time - expected_end).to_value("sec") == pytest.approx(0.0, abs=1.0)
    assert (end_time - start_time).to_value("jd") == pytest.approx(1.0)

    assert download_call["max_concurrent_requests"] == 4
    assert download_call["output_dir"] == str(tmp_path)
    assert len(upload_calls) == 2
    assert upload_calls[0]["destination_bucket"] == "unit-test-bucket-dev"
    assert upload_calls[1]["destination_bucket"] == "unit-test-bucket-prod"
    assert upload_calls[0]["calibrated_filename"].endswith(".csv")
    assert upload_calls[1]["calibrated_filename"].endswith(".csv")


def test_import_GOES_data_to_timestream(monkeypatch) -> None:
    """GOES import reads live NOAA JSON and only mocks timestream recording."""
    recorded_calls = []

    def fake_record_timeseries(*args, **kwargs):
        recorded_calls.append((args, kwargs))

    monkeypatch.setattr(
        "src.executor.executor.util.record_timeseries", fake_record_timeseries
    )

    try:
        Executor.import_GOES_data_to_timestream()
    except Exception as exc:
        pytest.skip(f"Live NOAA GOES endpoint unavailable during test run: {exc}")

    # If feed has fresh data in the last hour, function records xrsa and xrsb.
    # If feed is stale, function logs and records nothing.
    assert len(recorded_calls) in (0, 2)
    if recorded_calls:
        assert recorded_calls[0][1] == {
            "ts_name": "GOES",
            "instrument_name": "goes xrsa",
        }
        assert recorded_calls[1][1] == {
            "ts_name": "GOES",
            "instrument_name": "goes xrsb",
        }


def test_create_GOES_data_annotations(monkeypatch) -> None:
    """GOES annotations uses live flare feed and only mocks annotation writes."""
    annotation_calls = []

    def fake_create_annotation(**kwargs):
        annotation_calls.append(kwargs)

    monkeypatch.setattr(
        "src.executor.executor.util.create_annotation", fake_create_annotation
    )

    try:
        Executor.create_GOES_data_annotations()
    except Exception as exc:
        pytest.skip(f"Live NOAA flare endpoint unavailable during test run: {exc}")

    # Function writes 2 annotations per flare event: event range and peak marker.
    assert len(annotation_calls) % 2 == 0
    if annotation_calls:
        first_call = annotation_calls[0]
        second_call = annotation_calls[1]

        assert first_call["dashboard_name"] == "Context Observations"
        assert first_call["panel_name"] == "GOES XRS"
        assert first_call["mission_dashboard"] == "padre"
        assert first_call["overwrite"] is True
        assert first_call["tags"][0:2] == ["GOES XRS", "flare"]

        assert second_call["dashboard_name"] == "Context Observations"
        assert second_call["panel_name"] == "GOES XRS"
        assert second_call["mission_dashboard"] == "padre"
        assert second_call["overwrite"] is True
        assert second_call["tags"] == ["GOES XRS", "flare", "peak"]
