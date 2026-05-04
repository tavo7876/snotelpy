
import pytest
import pandas as pd
import xarray as xr
import geopandas as gpd
import numpy as np
import os




from snotelpy.fetch import (
    _parse_dates,
    _fetch_data,
    fetch_snotel,
    get_stations,
    station_info,
    save_data
)
from snotelpy.basin import basin_summary

#run all with : $pytest -v -m 

#run just api pulls with : $pytest -v -m integration

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

@pytest.mark.integration
def test_get_stations_huc_filter_returns_results():
    df = get_stations(hucs=["1019"])
    assert len(df) > 0




#---------------------------------------------------------------
#station_info()
#---------------------------------------------------------------
@pytest.mark.integration
def test_station_info_returns_list():
    result = station_info("663:CO:SNTL")
    assert isinstance(result, list)

@pytest.mark.integration  
def test_station_info_returns_known_station():
    result = station_info("663:CO:SNTL")
    assert result[0]["name"] == "Niwot"

# @pytest.mark.integration
# def test_station_info_has_location_fields():
#     result = station_info("663:CO:SNTL")
#     for field in result:
#         assert field in result[0] ==  "latitude"





#---------------------------------------------------------------
#fetch_snotel()
#---------------------------------------------------------------

@pytest.mark.integration
def test_fetch_returns_xarray_dataset():

        result = _fetch_data(
            stations=["602:CO:SNTL"],
            elements=["PREC"],
            duration="MONTHLY",
            start_date="2000-01-01",
            end_date="2001-01-01"
        )
        
        assert isinstance(result, xr.Dataset)



@pytest.mark.integration
def test_fetch_has_correct_dimensions(): 
    result = _fetch_data(
            stations=["602:CO:SNTL"],
            elements=["PREC"],
            duration="MONTHLY",
            start_date="2000-01-01",
            end_date="2001-01-01"
         )
       
    assert "time" in result.dims
    assert result.sizes == {'time': 13, 'station': 1}


@pytest.mark.integration
def test_fetch_has_correct_variable():
 
        
    result = _fetch_data(
            stations=["602:CO:SNTL"],
            elements=["PREC"],
            duration="MONTHLY",
            start_date="2000-01-01",
            end_date="2001-01-01"
            )
        
    assert "PREC" in result.data_vars



@pytest.mark.integration
def test_fetch_station_in_coords():

        
    result = _fetch_data(
            stations=["602:CO:SNTL"],
            elements=["PREC"],
            duration="MONTHLY",
            start_date="2000-01-01",
            end_date="2001-01-01"
            )
       
    assert "602:CO:SNTL" in result.coords['station'][0]


#-----------------------------
#basin_summary
#-----------------------------

@pytest.mark.integration
def test_basin_summary_returns_correct_len_and_type():
    basin_list = basin_summary(hucs = ['10190005'], 
                  elements = ['WTEQ'], 
                  start_date = "2020-10-01",
                  end_date = "2025-10-01",
                  climatology_period = ("2020-10-01","2025-10-01"))
    assert len(basin_list) == 3
    assert type(basin_list) is dict
    


#-----------------------------
#save_data
#-----------------------------
def test_save_data_dataset_creates_nc(tmp_path):
    ds = xr.Dataset(
        {"WTEQ": (["time", "station"], np.zeros((3, 1)))}
        )
    
    outfile = tmp_path / "test_output"
    save_data(ds, filename=str(outfile))
    assert os.path.exists(tmp_path)

def test_save_data_wrong_type_raises_error():
    with pytest.raises(TypeError):
        save_data("not a dataset") 
        
        

