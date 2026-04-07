import os
import sys
import math

import requests
import pandas as pd
import json
import yaml
import matplotlib.pyplot as plt
import xarray as xr
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

from snotelpy import Config 

# from my_project.utils import helper_function
# from my_project.constants import API_KEY




def _parse_dates(values, duration):
    
    """
    Private function to return datetime's based on duration and time values from api

    Parameters
    ----------
    values : list
        description
    duration : str
        duration from fetch, stripped and upper

    Returns
    -------
    pd.Datetimeindex
        returns a datetime index from start to end date for duration
    
    """
    
    df = pd.DataFrame(values)
    
    if duration.strip().upper() in ["DAILY", "HOURLY"]:
    
        dates = pd.to_datetime(df['date'])
        return pd.DatetimeIndex(dates, name="time")
        
        
    elif duration.strip().upper() == "SEMIMONTHLY":
       
        dates = pd.to_datetime(df['collectionDate'])
        return pd.DatetimeIndex(dates, name="time")
    
    elif duration.strip().upper() == "MONTHLY":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': df['month'],
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates, name="time")
        
    elif duration.strip().upper() == "CALENDAR_YEAR":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': 1, 
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates, name="time")
    elif duration.strip().upper() == "WATER_YEAR":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': 10, #a water year starts in october
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates, name="time")
  

def fetch_data(stations=[], elements="", duration="DAILY", start_date = "1991-01-01", end_date = "2100-01-01", include_coords = False): 
    '''
    Fetch data from the USDA AWDB REST API for one or more SNOTEL stations.

    Parameters
    ----------
    stations : list
        A list of station triplets in the format 'stationId:stateCode:networkCode'.
        Any portion of the triplet can contain the '*' wildcard character.
        Examples: ['602:CO:SNTL'], ['*:OR:SNTL', '*:WA:SNTL']
    elements : str
        A comma separated list of elements in the format elementCode:heightDepth:ordinal.
        Any part of the element string can contain the '*' wildcard character.
        HeightDepth and ordinal are optional, defaulting to null and 1 respectively.
        Examples: 'PREC', 'WTEQ', 'SMS:*', 'PREC::2'
    duration : str, optional
        The time duration of the data to retrieve, by default 'DAILY'.
        Options:
            - 'DAILY'
            - 'HOURLY'
            - 'SEMIMONTHLY'
            - 'MONTHLY'
            - 'CALENDAR_YEAR'
            - 'WATER_YEAR'
    start_date : str, optional
        Begin date in 'YYYY-MM-DD' format, by default '1991-01-01'.
        Relative dates accepted: 0 (current date) or -n where n corresponds to duration.
    end_date : str, optional
        End date in 'YYYY-MM-DD' format, by default '2100-01-01'.
        Relative dates accepted: 0 (current date) or -n where n corresponds to duration.

    Returns
    -------
    xr.Dataset
        An xarray Dataset with dimensions (time, station) containing the requested
        elements as data variables, with metadata attributes loaded from ELEMENTS.yaml.

    Raises
    ------
    ValueError
        If the API request fails or returns no data.
    '''
    
    
    url = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data"# base URL
    
    
    station_string = ",".join(stations)
    #all paramters striped of whitespace and all uppercased 
    params = {
        "stationTriplets": f"{station_string.strip().upper()}",
        "elements": f"{elements.strip().upper()}",
        "duration": f"{duration.strip().upper()}",
        "beginDate": f"{start_date.strip()}",
        "endDate": f"{end_date.strip()}",
    }
    
    response = requests.get(url, params= params) #requests api from base url with the params restful api
    
    #throw error if didnt retrive propraly 
    if response.status_code !=200:
        raise ValueError(f"API request failed with status code {response.status_code}: {response.text}")
        return
    
    
    # returns a list 
    data = response.json()
    if len(data) == 0:
        raise ValueError(f"API request was Successfull but contained no data: {data}")
        return
    
    
    #build a stationlist
    station_list = [station['stationTriplet'] for station in data]
    
    freq_map = {
        'DAILY' : "D",
        'HOURLY': "H",
        'SEMIMONTHLY': "SMS",
        'MONTHLY': "MS",
        'CALENDAR_YEAR' : "YS",
        'WATER_YEAR' : "YS-OCT"
    }
    
    last_date_values = data[0]['data'][0]['values'][-1]#finds the last date of the values from station 1(should be the right end date for all stations, 
    #i dont know what would happen if the sntl station is no longer active it may set the enddate to a date we dont want)
    last_date = _parse_dates([last_date_values],duration=duration)[0]#converts to a format based on what we need
   
    #looks in the freqmap for the value that matches the duration for the pd date range
    frequency = freq_map[duration.strip().upper()]
    dates = pd.date_range(start_date,last_date,freq=frequency)# builds are date time index 
    
    
    #call phrase dates(old)

    # values = data[0]['data'][0]['values']
    # print(values)
    # dates = _parse_dates(values, duration) #builds us a date time index based on the duration choosen 
   
    
    #building dat_vars loop
    with open(Config.ELEMENTS_YAML_PATH, 'r') as f:
        var_metadata = yaml.load(f, Loader=yaml.SafeLoader)
        
    data_vars = {}



    variables  = [var['stationElement']['elementCode'] for var in data[0]['data']]
 
    n_times    = len(dates)
    n_stations = len(station_list)
    
    
    #testing function 
    
    
    
    #loop
    #structure dictionary -> list -> dictionary -> list -> dictionaries.
    for element_code in variables: #loops through elementcodes ex: [PREC, WTEQ]
        # grid = np.zeros((n_times, n_stations))  # full grid upfront
        grid = np.full((n_times, n_stations), np.nan) # builds grid n_times, n_stations 
        # print(grid.shape) #402, 1 full of nan
        for i, station in enumerate(data): #access the data 
            for var in station['data']: #acesss 'data' witch is a List--> dictionary(stationelements) --> list('values')(all values are store)-->dict(each time)(each value)
                if var['stationElement']['elementCode'] == element_code:
                    df = pd.DataFrame(var['values'])   #creates a dataframe(table like, contains your time(ex for month __> month | year | value))
    
                      # build dates for THIS variable's values
                    var_dates = _parse_dates(var['values'], duration) #builds your date for the varibles values(needs to be insde the loop for each sation seperate dates)
                # match each value to its position in master dates
                for j, d in enumerate(var_dates): # if d is in dates 
                    if d in dates: # if d in the varibles(prec, wteq, ect...)dates matches with the dates values in the stations first element, then:
                        row_idx = dates.get_loc(d) #get the location(index of that date in dates 
                        grid[row_idx, i] = df['value'].values[j]   #match idex of grid w/ [row_idx = value for that date, station(1,2,3 ect)]
                    #possibilty to break if prec and wteq or other elements have differnt readings(but im not sure)
                   
    
        data_vars[element_code] = (["time", "station"], grid) #build the dictionary array coresponding to the data 
    
    var_dates = _parse_dates(var['values'], duration)
    
    
        
    
    #build the data set
    ds = xr.Dataset(
        data_vars = data_vars,
        coords={
            "time": dates,
            "station": station_list
        }
    )
    
    #adding lat/lon, and elevation into the xarray, 
    if include_coords:
        df = get_stations(stations,elements=elements,returnType='pd')
        df_aligned = df.set_index('stationTriplet').loc[stations]
        
        ds = ds.assign_coords(
        latitude=("station",df_aligned['latitude'] ),
        longitude=("station",df_aligned['longitude'] ),
        elevation=("station", df_aligned['elevation'] )
        )
        ds.coords['elevation'].attrs['units'] = 'feet'
        ds.coords['latitude'].attrs['units'] = 'latitude'
        ds.coords['longitude'].attrs['units'] = 'longitude'
    

    for var_name in ds.data_vars: #stores metadate from ELEMENTS.yaml
        if var_name in var_metadata:
            ds[var_name].attrs = var_metadata[var_name]
    
    return ds
            

    
def station_info(station_triplet="",): 
    '''
    Get station metadata from USDA AWDB REST API for one SNOTEL stations. 
    
    Parameters
    ----------
    station_triplet : str
        A string of a station triplet in the format 'stationId:stateCode:networkCode'
        Any portion of the triplet can contain the '*' wildcard character.
        Examples: '602:CO:SNTL'


    Returns
    -------
    list
        A nested list that has info for request station. 
    
    
    
    '''
    
    URL = 'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations'
    
    params = {
    "stationTriplets": f"{station_triplet.strip().upper()}",
    }
    request = requests.get(URL,params=params )
    
    return request.json()


def get_stations(station_triplets =["::SNTL"], elements = "", hucs = [], county_name ="", station_name = "",returnStationElements = "false",returnType = 'pd'):
    """
    Retrieve SNOTEL station metadata from the USDA AWDB REST API.

    Parameters
    ----------
    station_triplets : List, optional
        A comma separated list of station triplets in the format 
        'stationId:stateCode:networkCode', by default '::SNTL' (all SNOTEL stations).
        Any portion of the triplet can contain the '*' wildcard character.
        Examples: '602:CO:SNTL', '*:CO:SNTL'
    elements : str, optional
        Filter stations by element code, by default '' (no filter).
        Examples: 'PREC', 'WTEQ'
    hucs : list of str, optional
        Filter stations by HUC watershed code, by default [] (no filter).
    county_name : str, optional
        Filter stations by county name, by default '' (no filter).
        Example: 'Boulder'
    station_name : str, optional
        Filter stations by station name, by default '' (no filter).
    returnStationElements : str, optional
        Whether to return station elements, by default 'false'.
    returnType : str, optional
        The type of object to return, by default 'pd'.
        Options:
            - 'pd' : pandas DataFrame
            - 'gpd' : geopandas GeoDataFrame with point geometry (EPSG:4326)

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        Station metadata including name, triplet, elevation, latitude, and longitude.
        Returns a GeoDataFrame with point geometry if returnType='gpd'.

    Raises
    ------
    ValueError
        If the API request fails.
    """
    
    URL = 'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations'
    station_string = ",".join(station_triplets)
    params = {
    "stationTriplets": f"{station_string.strip().upper()}",
    "elements": f"{elements.strip().upper()}",
    "hucs": hucs,
    "countyNames": f"{county_name.strip().upper()}",
    "stationNames": f"{station_name.strip().upper()}",
    "returnStationElements": f"{returnStationElements}",
    }
    
    request = requests.get(URL,params=params )
    
    data = request.json() 
    
    
    #throw error if didnt retrive propraly 
    if request.status_code !=200:
        raise ValueError(f"API request failed with status code {request.status_code}: {request.text}")
        return
    # returns a list 
    data = request.json()
    if len(data) == 0:
        raise ValueError(f"API request was Successfull but contained no data: {data}")
        return
    
    df = pd.DataFrame(data)
    
    if returnType.strip().upper() == "GPD":
        gdf = gpd.GeoDataFrame(df,
            geometry= gpd.points_from_xy(df.longitude,df.latitude),
            crs = "EPSG:4326" )

        return gdf
    else:
        return df
    
    
    
        
        
        
        
        
        
       
    
   

    






if __name__ == "__main__": #main header gaurder  
    
    station1 = "602:CO:sntl, 617:AZ:SNTL"
    elements = "PREC"
    
    # ds = fetch_data(station1,elements  ,duration="MONTHLY")
    
    # print(ds)
    # ds['PREC'].isel(station=0).plot()
    
    # print(ds)
    # plt.show()
    
    # data = station_info(":CO:SNTL")
  
    # print(data[0]['latitude'])
    # print(data[0]['elevation'])
    
    gdf = get_stations(county_name = "Boulder",returnType='gpd')
    print(gdf)

    # print(df[df['elevation'] > 9000])
    # station_boulder_highest = df[df['elevation'] > 9000]['stationTriplet']
    # list1 = station_boulder_highest.to_list()
    
    
    # ds = fetch_data(list1, "PREC", "CALENDAR_YEAR")
    
    # ds['PREC'].isel(station = 0).plot(label = 'station 0')
    # ds['PREC'].isel(station = 1).plot(label = 'station 1')
    # ds['PREC'].isel(station = 2).plot(label = 'station 2')
    # ds['PREC'].isel(station = 3).plot(label = 'station 3')
    # ds['PREC'].isel(station = 4).plot(label = 'station 4')
    # plt.legend()
    # plt.show()
   