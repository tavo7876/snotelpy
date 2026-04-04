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

from snotelpy import Config 

# from my_project.utils import helper_function
# from my_project.constants import API_KEY




def _parse_dates(values, duration):
    
    """
    Parse dates from AWDB API values list based on duration type.
    Returns a pandas DatetimeIndex.
    """
    
    df = pd.DataFrame(values)
    
    if duration.strip().upper() in ["DAILY", "HOURLY"]:
    
        dates = pd.to_datetime(df['date'])
        return pd.DatetimeIndex(dates)
        
        
    elif duration.strip().upper() == "SEMIMONTHLY":
       
        dates = pd.to_datetime(df['collectionDate'])
        return pd.DatetimeIndex(dates)
    
    elif duration.strip().upper() == "MONTHLY":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': df['month'],
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates)
        
    elif duration.strip().upper() == "CALENDAR_YEAR":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': 1, 
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates)
    elif duration.strip().upper() == "WATER_YEAR":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': 10, #a water year starts in october
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates)
  

def fetch_data(stations=[], elements="", duration="DAILY", start_date = "1991-01-01", end_date = "2100-01-01"): 
    '''
    # Enter a station
    A comma separated list of station triplets (ie. stationId:stateCode:networkCode). Any portion of the triplet can contain the '*' wildcard character. For example, if you want all SNOTEL stations in OR or WA, you can request '*:OR:SNTL, *:WA:SNTL'.
    
    # Enter Elements:
    A comma separated list of elements in the format elementCode:heightDepth:ordinal. Any part of the element string can contain the '*' wildcard character. The heightDepth and ordinal are optional. If heightDepth is not specified then it is assumed to be null. If the ordinal is not specified, then it is assumed to be 1. The heightDepth is in inches. Examples: PREC, WTEQ, SMS:*, PREC::2
    
    
    
    # Duration =
    DAILY \n
    HOURLY \n
    SEMIMONTHLY \n
    MONTHLY \n
    CALENDAR_YEAR \n
    WATER YEAR \n
                
                
    ## Datetime = 'YYYY-MM-DD'
    The begin date of the data to retrieve. Expects either a date or a relative date. The date must be in the 'yyyy-MM-dd HH:mm' or 'MM/dd/yyyy HH:mm' format (time defaults to midnight when not specified). Relative dates can be 0 (the current date) or -n where the unit of n corresponds to the duration.
   

    \nThe end date of the data to retrieve. Expects either a date or a relative date. The date must be in the 'yyyy-MM-dd HH:mm' or 'MM/dd/yyyy HH:mm' format (time defaults to midnight when not specified). Relative dates can be 0 (the current date) or -n where the unit of n corresponds to the duration.
    \nDefaluts are from 1991-10-01 to 2100-01-01
    
    # Output
    output --> what type of data to convert to, 
    pd = pandas dataframe
    xr = xarray dataset
    
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
    
    #call phrase dates

    values = data[0]['data'][0]['values']
    dates = _parse_dates(values, duration) #builds us a date time index based on the duration choosen 
   
    
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
    
    for j, d in enumerate(var_dates):
        print(f"looking for: {d}, type: {type(d)}")
        print(f"in dates: {d in dates}")
        break
        
    
    #build the data set
    ds = xr.Dataset(
        data_vars = data_vars,
        coords={
            "time": dates,
            "station": station_list
        }
    )

    for var_name in ds.data_vars: #stores metadate from ELEMENTS.yaml
        if var_name in var_metadata:
            ds[var_name].attrs = var_metadata[var_name]
    
    return ds
            

    
def station_info(station_triplet="",): 
    '''
    
    # This function returns a dict of a stations metadata
    
    Enter a station triplet a comma separated list of station triplets (ie. stationId:stateCode:networkCode) as a string.
    
    Will return a nested list inside of a dictonary. 
    
    
    '''
    
    URL = 'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations'
    
    params = {
    "stationTriplets": f"{station_triplet.strip().upper()}",
    }
    request = requests.get(URL,params=params )
    
    return request.json()


def get_stations(station_triplets ="::SNTL", elements = "", hucs = "", county_name ="", station_name = "",returnStationElements = "false"):
    '''
    
    '''
    
    URL = 'https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations'
    
    params = {
    "stationTriplets": f"{station_triplets.strip().upper()}",
    "elements": f"{elements.strip().upper()}",
    "hucs": f"{hucs.strip()}",
    "countyNames": f"{county_name.strip().upper()}",
    "stationNames": f"{station_name.strip().upper()}",
    # "returnStationElements": "false"
    }
    
    request = requests.get(URL,params=params )
    
    data = request.json()  
    df = pd.DataFrame(data)
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
    
    df = get_stations(county_name = "Boulder")
    print(df)

    print(df[df['elevation'] > 9000])
    station_boulder_highest = df[df['elevation'] > 9000]['stationTriplet']
    list1 = station_boulder_highest.to_list()
    
    
    ds = fetch_data(list1, "PREC", "CALENDAR_YEAR")
    
    ds['PREC'].isel(station = 0).plot(label = 'station 0')
    ds['PREC'].isel(station = 1).plot(label = 'station 1')
    ds['PREC'].isel(station = 2).plot(label = 'station 2')
    ds['PREC'].isel(station = 3).plot(label = 'station 3')
    ds['PREC'].isel(station = 4).plot(label = 'station 4')
    plt.legend()
    plt.show()
   