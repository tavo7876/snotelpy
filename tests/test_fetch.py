import pytest
import xarray as xr
from unittest.mock import MagicMock

from snotelpy.fetch import _fetch_data, fetch_snotel


# ============================================================
# Dataset structure  (no network — uses sample_dataset fixture)
# ============================================================

def test_returns_xarray_dataset(sample_dataset):
    assert isinstance(sample_dataset, xr.Dataset)


def test_dataset_has_correct_shape(sample_dataset):
    assert sample_dataset.sizes == {"time": 39, "station": 2}


def test_dataset_has_expected_variables(sample_dataset):
    assert "PREC" in sample_dataset.data_vars
    assert "WTEQ" in sample_dataset.data_vars


def test_station_coordinate_matches_input(sample_dataset):
    stations = list(sample_dataset.coords["station"].values)
    assert "602:CO:SNTL" in stations
    assert "936:CO:SNTL" in stations


def test_time_coordinate_starts_on_requested_date(sample_dataset):
    assert str(sample_dataset.coords["time"].values[0])[:10] == "2026-03-25"


def test_dataset_has_source_attribute(sample_dataset):
    assert sample_dataset.attrs["source"] == "USDA NRCS AWDB REST API"


def test_dataset_duration_attribute_is_uppercased(sample_dataset):
    assert sample_dataset.attrs["duration"] == "DAILY"


# ============================================================
# Value correctness  — known values from the mock payload
# ============================================================

def test_known_prec_value_lands_in_correct_grid_position(sample_dataset):
    # 602:CO:SNTL PREC on 2026-03-25 is 10.7 in the mock data
    val = sample_dataset["PREC"].sel(station="602:CO:SNTL", time="2026-03-25").item()
    assert val == pytest.approx(10.7)


def test_zero_value_is_not_treated_as_nan(sample_dataset):
    # 936:CO:SNTL WTEQ on 2026-03-31 is 0.0 — must not become NaN
    val = sample_dataset["WTEQ"].sel(station="936:CO:SNTL", time="2026-03-31").item()
    assert val == pytest.approx(0.0)


def test_second_station_prec_value_is_correct(sample_dataset):
    # 936:CO:SNTL PREC on 2026-03-25 is 6.9 in the mock data
    val = sample_dataset["PREC"].sel(station="936:CO:SNTL", time="2026-03-25").item()
    assert val == pytest.approx(6.9)


# ============================================================
# Error handling
# ============================================================

def test_http_error_raises_value_error(monkeypatch):
    mock = MagicMock()
    mock.status_code = 404
    mock.text = "Not Found"
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    with pytest.raises(ValueError, match="404"):
        _fetch_data(
            stations=["602:CO:SNTL"],
            elements=["PREC"],
            duration="DAILY",
            start_date="2026-03-25",
            end_date="2026-05-02",
        )


def test_empty_response_raises_value_error(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = []
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    with pytest.raises(ValueError):
        _fetch_data(
            stations=["602:CO:SNTL"],
            elements=["PREC"],
            duration="DAILY",
            start_date="2026-03-25",
            end_date="2026-05-02",
        )


# ============================================================
# Input tolerance
# ============================================================

def test_lowercase_duration_accepted(monkeypatch, mock_api_response):
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock_api_response))
    ds = _fetch_data(
        stations=["602:CO:SNTL", "936:CO:SNTL"],
        elements=["PREC", "WTEQ"],
        duration="daily",
        start_date="2026-03-25",
        end_date="2026-05-02",
    )
    assert isinstance(ds, xr.Dataset)


def test_duration_with_leading_whitespace_accepted(monkeypatch, mock_api_response):
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock_api_response))
    ds = _fetch_data(
        stations=["602:CO:SNTL", "936:CO:SNTL"],
        elements=["PREC", "WTEQ"],
        duration=" DAILY ",
        start_date="2026-03-25",
        end_date="2026-05-02",
    )
    assert isinstance(ds, xr.Dataset)


# ============================================================
# fetch_snotel — single-call path (small request)
# ============================================================

def test_fetch_snotel_small_request_returns_dataset(monkeypatch, mock_api_response):
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock_api_response))
    monkeypatch.setattr("snotelpy.fetch._chunkgen", MagicMock(return_value=(1, 100)))
    ds = fetch_snotel(
        stations=["602:CO:SNTL", "936:CO:SNTL"],
        elements=["PREC", "WTEQ"],
        duration="DAILY",
        start_date="2026-03-25",
        end_date="2026-05-02",
    )
    assert isinstance(ds, xr.Dataset)


# ============================================================
# Integration  (hits the real AWDB API — requires network)
# ============================================================

@pytest.mark.integration
def test_integration_fetch_returns_dataset():
    ds = _fetch_data(
        stations=["602:CO:SNTL"],
        elements=["PREC"],
        duration="MONTHLY",
        start_date="2000-01-01",
        end_date="2001-01-01",
    )
    assert isinstance(ds, xr.Dataset)


@pytest.mark.integration
def test_integration_fetch_has_correct_variable():
    ds = _fetch_data(
        stations=["602:CO:SNTL"],
        elements=["PREC"],
        duration="MONTHLY",
        start_date="2000-01-01",
        end_date="2001-01-01",
    )
    assert "PREC" in ds.data_vars
