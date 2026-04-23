from snotelpy.fetch import fetch_snotel, get_stations
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt

    
def basin_summary(hucs=None, 
                  elements=["WTEQ", "PREC", "SNWD", "TAVG", "TMAX", "TMIN"], 
                  duration="Daily", 
                  start_date="1991-01-01", 
                  end_date=None, 
                  climatology_period=("1991-10-01", "2020-09-30")):
    
    '''
    Retrive a basin summary from the USDA AWDB REST API for a SNOTEL watershed basin denoted by Hydrologic Unit Code[HUC]. 
    For information on HUC codes and how they are structured vist <https://nas.er.usgs.gov/hucs.aspx>
    
    Parameters
    ----------
    hucs : list
        A list containg a HUC values from digits 2 to 12. 
        Examples: [10], [1019]
        Refer to <https://nas.er.usgs.gov/hucs.aspx> for HUC structure. 
    elements : list
        A comma separated list of elements in the format elementCode:heightDepth:ordinal.
        Any part of the element string can contain the '*' wildcard character.
        HeightDepth and ordinal are optional, defaulting to null and 1 respectively.
        Examples: ['PREC', 'WTEQ'], 'SMS:*', 'PREC::2'
    duration : str, optional
        The time duration of the data to retrieve, by default 'DAILY'.
        Options:
            - 'DAILY'
            - 'MONTHLY'
        Note: Basin stats is the only duration effected data. Climatology is based on a 12 month average.  
        
    start_date : str, optional
        Begin date in 'YYYY-MM-DD' format, by default '1991-01-01'.

    end_date : str, optional
        End date in 'YYYY-MM-DD' format, by default '2100-01-01'.
    
   climatology_period: tuple, optional
        Foramt of tuple is start of climatology to end of climatology. Datetime format 'YYYY-MM-DD'. 
        By default ("1991-10-01", "2025-09-30")):
        This climatology has to fall within the dates of start_date and end_date. It wont throw a error but is important to consider when requesting.  
   

    Returns
    -------
    result : dict
        Dictionary containing three keys:

        basin_stats : xarray.Dataset
            Basin-averaged statistics across all stations in the requested
            HUC(s), computed by the AWDB API.

            Dimensions: (stat: 4, time: N)
                - ``stat``: 'MEAN', 'MEDIAN', 'MAX', 'MIN'
                - ``time``: datetime64, frequency matches the ``duration``
                  argument ('DAILY' or 'MONTHLY').

            Data variables: one per element requested (e.g. 'WTEQ', 'PREC'),
            each with shape (stat, time) and dtype float64.

            Attributes: ``source``, ``network``, ``duration``, ``hucs``,
            ``Number of Stations``, ``Description``.

        climatology : xarray.Dataset
            Monthly climatology of basin-averaged values over
            ``climatology_period``. Always monthly, regardless of the
            ``duration`` argument.

            Dimensions: (climatology: 2, month: 12)
                - ``climatology``: 'MEAN', 'MEDIAN' (note: no MAX/MIN).
                - ``month``: integers 1–12.

            Data variables: one per element requested, each with shape
            (climatology, month) and dtype float64.

            Attributes: same keys as ``basin_stats``, with ``duration``
            set to 'monthly climatology'.

        stations : geopandas.GeoDataFrame
            All SNOTEL stations located within the requested HUC(s),
            as returned by ``get_stations``.

            Key columns: ``stationTriplet``, ``name``, ``stateCode``,
            ``elevation``, ``latitude``, ``longitude``, ``beginDate``,
            ``endDate``, ``huc``, ``associatedHucs``, ``geometry``.

            Geometry: POINT in EPSG:4326 (longitude, latitude).

    Raises
    ------
    ValueError
        If the API request fails or returns no data.
    Notes
    For large requests, data is automatically fetched in multiple chunks 
    and concatenated. Basins with no data for a given chunk will contain NaN values.
    
    
    '''
    if hucs is None:
        raise ValueError("No Specified Basin HUC")
    if end_date is None:
        end_date = "2100-01-01"
    if not hucs:
        raise ValueError("Hucs was empty or basin couldnt be requested")
        
    gdf = get_stations(hucs= hucs, returnType='gpd')
    
    ds = fetch_snotel(gdf['stationTriplet'].tolist(),elements=elements, duration = duration ,start_date= start_date, end_date=end_date)
    
    n_stations = len(ds['station'])

   
    
    
    #basin stats 
    
    basin_mean = ds.mean(dim='station')
    basin_median = ds.median(dim = 'station')
    basin_max = ds.max(dim = 'station')
    basin_min = ds.min(dim = 'station')
    
    basin_stats = xr.concat([basin_mean,basin_median,basin_max,basin_min],dim = pd.Index(["MEAN", "MEDIAN", "MAX", "MIN"],dtype="object", name='stat'))
    basin_stats.attrs['hucs'] = hucs
    basin_stats.attrs['Number of Stations'] = n_stations
    basin_stats.attrs['Description'] = f"Basin-averaged statistics for HUC: {hucs}"
 
 
 
   
    #climatology
    climatology_start = climatology_period[0]
    climatology_end = climatology_period[1]
    climatology_mean = basin_stats.sel(stat='MEAN', time=slice(climatology_start,climatology_end)).groupby("time.month").mean().drop_vars('stat')
    climatology_median = basin_stats.sel(stat='MEDIAN', time=slice(climatology_start,climatology_end)).groupby("time.month").mean().drop_vars('stat') # we use .mean here because its the median of all values over the mean of jan or aprl...ect

    climatology = xr.concat([climatology_mean, climatology_median],dim = pd.Index(["MEAN","MEDIAN"],dtype="object", name = 'climatology'))
    climatology.attrs['hucs'] = hucs
    climatology.attrs['Number of Stations'] = n_stations
    climatology.attrs['duration'] = "monthly climatology"
    climatology.attrs['Description'] = f"Monthly Climatology for HUC: {hucs}"
    
    
    # Percent of median for WTEQ:
    
    # clima_df = climatology.sel(climatology = 'MEDIAN').to_pandas()
    # stats_df = basin_stats['WTEQ'].sel(stat = 'MEAN').to_pandas()
    
   
    # stats_monthly = stats_df.resample('ME').mean()
   
    # month_ints = stats_monthly.index.month
    
    # clim_series = clima_df['WTEQ']

    # print(month_ints)
    # clim_values = month_ints.map(clim_series)
    # print(clim_values)
    
    # pct_of_median = (stats_monthly / clim_values.values) * 100
    # print(pct_of_median)
    # pct_of_median = pct_of_median.where(clim_series > 0.5)
    
    
    
    return {
        "basin_stats": basin_stats, 
        "climatology": climatology,
        # "percent_of_median": percent_of_median, #will add this function later hopefully. 
        "stations": gdf  
    }

    

    
    



if __name__ == "__main__":
    out = basin_summary(hucs=[1019], 
                            start_date = "2000-10-01",
                            end_date= "2025-10-01", 
                            elements=['WTEQ'],
                            climatology_period=("2000-10-01", "2025-10-01"),
                            duration = "MONTHLY"
                            )
                           
    print(out["basin_stats"])
    print(out["climatology"])
    print(out["stations"].head())
    print(out["stations"].columns.tolist())                     

   
    
    # print(basin_sum)
    
    
    
    # print(basin_stats['WTEQ'].sel(stat = 'MEAN').values.max())
    # percent_of_median = basin_sum['percent_of_median']
    # print(percent_of_median)
    # basin_stats['WTEQ'].sel(stat = 'MEAN').plot()
    # percent_of_median.plot()
    # print(percent_of_median.max().values)
    # print(percent_of_median.min().values)
    
    # plt.show()
    