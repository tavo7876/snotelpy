import os
import sys
import math

import pandas as pd
import requests
import xarray as xr
import json
import yaml
import numpy as np
import Config 

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
        return dates
    elif duration.strip().upper() == "WATER_YEAR":
        dates = pd.to_datetime({
            'year': df['year'],
            'month': 10, #a water year starts in october
            'day': 1 
            
                        })
        return pd.DatetimeIndex(dates)
  


"1991-10-01" "2100-01-01"

def fetch_data(stations="", elements="", duration="DAILY", start_date = "1991-01-01", end_date = ""): 
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
    
    #all paramters striped of whitespace and all uppercased 
    params = {
        "stationTriplets": f"{stations.strip().upper()}",
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
    
    
    
    
    
    for element_code in variables:
        grid = np.zeros((n_times, n_stations))  # full grid upfront
    
        for i, station in enumerate(data):
            for var in station['data']:
                if var['stationElement']['elementCode'] == element_code:
                    
                    
                    
                    df = pd.DataFrame(var['values'])      
                    grid[:, i] = df['value'].values 
    
    data_vars[element_code] = (["time", "station"], grid)

    #build the data set
    ds = xr.Dataset(
        data_vars = data_vars,
        coords={
            "time": dates,
            "station": station_list
        }
    )

    for var_name in ds.data_vars:
        if var_name in var_metadata:
            ds[var_name].attrs = var_metadata[var_name]
    
    return ds
            
    
    
    
        
        
        
        
        
        
        
        
        
       
    
   

    






if __name__ == "__main__": #main header gaurder  
    station1 = "602:CO:sntl "
    elements = "PREC, wteq "
    response = fetch_data(station1, elements, "Monthly")
    print(response)
    
    
    
    


 