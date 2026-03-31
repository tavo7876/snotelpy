import xarray as xr
import requests 
import json
import pandas as pd




URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1"
#all colorado stations, element SMS, all dates
# https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=%3ACO%3ASNTL&elements=SMS%3A&returnFlags=false&returnOriginalValues=false&returnSuspectData=false
#All colorado, element PREC, all Dates
# https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=%3ACO%3ASNTL&elements=PREC&returnFlags=false&returnOriginalValues=false&returnSuspectData=false
# 602 CO, PREC, all dates
# https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=602%3ACO%3ASNTL&elements=PREC&returnFlags=false&returnOriginalValues=false&returnSuspectData=false
#602 CO, PREC and WTEQ, all dates
# https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=602%3ACO%3ASNTL&elements=PREC%2C%20WTEQ&returnFlags=false&returnOriginalValues=false&returnSuspectData=false#stationId:stateCode:networkCode
#all OR and WA, PREC, all Dates
# https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=%3AOR%3ASNTL%2C%20%3AWA%3ASNTL&elements=PREC&returnFlags=false&returnOriginalValues=false&returnSuspectData=false
#602 CO, PREC, 2000-01-01 --> 2001-01-01, monthly
#https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=602%3ACO%3ASNTL&elements=PREC&duration=MONTHLY&beginDate=2000-01-01&endDate=2001-01-01&returnFlags=false&returnOriginalValues=false&returnSuspectData=false
# # PREC%2C%20SMS%2C%20WTEQ

#stationids form "id%3Astate%3ASNTL"
def fetch_data(stations=[""], varibles=[""], duration="DAILY", start_date = "1991-10-01", end_date = "2100-01-01", output= "pd"): 
    '''
    Enter a station[:CO:SNTL] or station[602:CO:SNTL] or station[602:CO:SNTL, :WA ]
    in format        %3ACO                  602%3ACO             %3ACO
    
    Duration =  DAILY
                HOURLY
                SEMIMONTHLY
                MONTHLY
                CALENDAR_YEAR
                WATER YEAR
    Datetime = 'YYYY-MM-DD'
        Defaluts are from 1991-10-01 to 2100-01-01
        
    output --> what type of data to convert to, 
    pd = pandas dataframe
    xr = xarray dataset
    
    '''
    
    varibles_length = len(varibles)
    station_length = len(stations)

    if station_length == 1:
        if varibles_length == 1:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL&elements={varibles[0]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        elif varibles_length == 2:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL&elements={varibles[0]}%2C%20{varibles[1]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        elif varibles_length == 3:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL&elements={varibles[0]}%2C%20{varibles[1]}%2C%20{varibles[2]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        elif varibles_length == 4:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL&elements={varibles[0]}%2C%20{varibles[1]}%2C%20{varibles[2]}%2C%20{varibles[3]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        else:
            print("To many varibles")
    elif station_length == 2:
        #                       https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets=602%3ACO%3ASNTL%2C%20617%3AAZ%3ASNTL&elements=SMS%3A&returnFlags=false&returnOriginalValues=false&returnSuspectData=false
        #%3AOR%3ASNTL%2C%20%3AWA%3ASNTL                                                   stationTriplets=%3AOR%3ASNTL%2C%20%3AWA%3ASNTL  602%3ACO        
        if varibles_length == 1:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL%2C%20{stations[1]}%3ASNTL&elements={varibles[0]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        elif varibles_length == 2:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL%2C%20{stations[1]}%3ASNTL&elements={varibles[0]}%2C%20{varibles[1]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        elif varibles_length == 3:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL%2C%20{stations[1]}%3ASNTL&elements={varibles[0]}%2C%20{varibles[1]}%2C%20{varibles[2]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        elif varibles_length == 4:
            ds = requests.get(f"https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data?stationTriplets={stations[0]}%3ASNTL%2C%20{stations[1]}%3ASNTL&elements={varibles[0]}%2C%20{varibles[1]}%2C%20{varibles[2]}%2C%20{varibles[3]}&duration={duration}&beginDate={start_date}&endDate={end_date}&returnFlags=false&returnOriginalValues=false&returnSuspectData=false")
        else:
            print("To many varibles")
    
    if ds.status_code == 200: 
        return ds       
    else:
        print(f"Didnt Load data correctly{ds.status_code}")




if __name__ == "__main__": #main header gaurder  
    
    #test functions 
    # id1 = "602%3ACO"#602 CO
    # id2 = ["%3AOR", "%3AWA"]
    id3 = ["602%3ACO", "617%3AAZ"]
    id4 = ["602%3ACO"]
    # varible_1 = ["PREC", "WTEQ", "TMAX"]
    varible_2 = ["PREC"]
    response = fetch_data(id4, varible_2, duration= "MONTHLY",start_date= "2000-01-01", end_date="2001-01-01")
    
    
    
    


        
 