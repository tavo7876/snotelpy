
import pytest
import pandas as pd
import xarray as xr
import geopandas as gpd


from snotelpy.fetch import (
    _parse_dates,
    _fetch_data,
    fetch_snotel,
    get_stations,
    station_info,
)
# ============================================================
#_parse_dates
# ============================================================

def test_monthly_returns_datetimeindex():
    values = [{"year" : 2020, "month": 1, "value": 5.0}]

    result = _parse_dates(values, "MONTHLY")
    assert isinstance(result, pd.DatetimeIndex)

def test_monthly_day_is_always_first():
    values = [{"year" : 2020, "month": 1, "value": 5.0}]
    result = _parse_dates(values, "MONTHLY")
    assert result[0].day == 1
    
def test_parse_dates_monthly_correct_length():
    values = [
        {"year": 2020, "month": 1, "value": 5.0},
        {"year": 2020, "month": 2, "value": 6.0},
        {"year": 2020, "month": 3, "value": 7.0},
    ]
    result = _parse_dates(values, "MONTHLY")
    assert len(result) == 3
    
def test_parse_dates_calendar_year_starts_jan():
    values = [{"year" : 2020, "value": 5.0}]
    result = _parse_dates(values, "CALENDAR_YEAR")
    assert result[0].month == 1
    assert result[0].day == 1

def test_parse_dates_water_year_starts_october():
    values = [{"year" : 2020, "value": 5.0}]
    result = _parse_dates(values, "WATER_YEAR")
    assert result[0].month == 10
    assert result[0].day == 1
    
def test_parse_dates_daily_returns_datetimeindex_and_Correct_Length():
    values = [
        {"date" : "2020-01-01", "value": 5.0},
        {"date" : "2020-01-02", "value": 6.0}
    ]
    result = _parse_dates(values, "DAILY")
    assert isinstance(result, pd.DatetimeIndex)
    assert len(result) == 2


# ============================================================
#get_stations()
# ============================================================
@pytest.mark.integration

def test_get_stations_returns_dataframe():
    df = get_stations(county_name="Boulder")
    assert isinstance(df, pd.DataFrame)
    

@pytest.mark.integration
def test_get_stations_returns_geodataframe():
    gdf = get_stations(county_name="Boulder",returnType = 'gpd')
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert "geometry" in gdf.columns
    
@pytest.mark.integration
def test_get_stations_has_request_collumns():
    df = get_stations(county_name="Boulder")
    expected = ["stationTriplet", "latitude", "longitude", "elevation"]
    for col in expected:
        assert col in df.columns
        