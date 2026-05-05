# get_stations + station_info
import pytest
import pandas as pd
import geopandas as gpd
from unittest.mock import MagicMock
from snotelpy.fetch import get_stations, station_info


# ---------------------------------------------------------------------------
# Mock station payload — what the AWDB stations endpoint returns
# Two stations with the fields get_stations uses to build its DataFrame
# ---------------------------------------------------------------------------
MOCK_STATION_DATA = [
    {
        "stationTriplet": "602:CO:SNTL",
        "name": "Berthoud Summit",
        "latitude": 39.7983,
        "longitude": -105.7797,
        "elevation": 11306,
    },
    {
        "stationTriplet": "663:CO:SNTL",
        "name": "Niwot",
        "latitude": 40.0526,
        "longitude": -105.5797,
        "elevation": 11600,
    },
]


# ============================================================
# get_stations — return type
# ============================================================

def test_get_stations_returns_dataframe(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = MOCK_STATION_DATA
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = get_stations(station_triplets=["602:CO:SNTL", "663:CO:SNTL"])
    assert isinstance(result, pd.DataFrame)


def test_get_stations_gpd_returns_geodataframe(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = MOCK_STATION_DATA
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = get_stations(station_triplets=["602:CO:SNTL", "663:CO:SNTL"], returnType="gpd")
    assert isinstance(result, gpd.GeoDataFrame)


# ============================================================
# get_stations — DataFrame contents
# ============================================================

def test_get_stations_correct_row_count(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = MOCK_STATION_DATA
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = get_stations(station_triplets=["602:CO:SNTL", "663:CO:SNTL"])
    assert len(result) == 2


def test_get_stations_has_expected_columns(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = MOCK_STATION_DATA
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = get_stations(station_triplets=["602:CO:SNTL", "663:CO:SNTL"])
    for col in ["stationTriplet", "latitude", "longitude", "elevation", "name"]:
        assert col in result.columns

def test_get_stations_gpd_has_geometry_column(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = MOCK_STATION_DATA
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = get_stations(station_triplets=["602:CO:SNTL", "663:CO:SNTL"], returnType="gpd")
    assert 'geometry' in result.columns


# ============================================================
# get_stations — error handling
# ============================================================

def test_get_stations_http_error_raises_value_error(monkeypatch):
    mock = MagicMock()
    mock.status_code = 404
    mock.text = "Not Found"
    mock.json.return_value = []
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    with pytest.raises(ValueError):
        get_stations(
            station_triplets= ['602:CO:SNTL', '663:CO:SNTL']
        )


def test_get_stations_empty_response_raises_value_error(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = []
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    with pytest.raises(ValueError):
        get_stations(
                station_triplets= ['602:CO:SNTL', '663:CO:SNTL']
            )


# ============================================================
# station_info
# ============================================================

def test_station_info_returns_list(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = [MOCK_STATION_DATA[0]]
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = station_info("602:CO:SNTL")
    assert isinstance(result, list)


def test_station_info_returns_correct_name(monkeypatch):
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = [MOCK_STATION_DATA[0]]
    monkeypatch.setattr("snotelpy.fetch.requests.get", MagicMock(return_value=mock))
    result = station_info("602:CO:SNTL")
    assert result[0]["name"] == "Berthoud Summit"


# ============================================================
# Integration  (hits the real AWDB API — requires network)
# ============================================================

@pytest.mark.integration
def test_integration_get_stations_returns_dataframe():
    df = get_stations(county_name="Boulder")
    assert isinstance(df, pd.DataFrame)


@pytest.mark.integration
def test_integration_get_stations_has_expected_columns():
    df = get_stations(county_name="Boulder")
    for col in ["stationTriplet", "latitude", "longitude", "elevation"]:
        assert col in df.columns


@pytest.mark.integration
def test_integration_station_info_returns_known_station():
    result = station_info("663:CO:SNTL")
    assert result[0]["name"] == "Niwot"
