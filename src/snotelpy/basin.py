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
    Returns dict of datasets, with the three keys "basin_stats", "climatology", "stations"
    
    '''
    if hucs is None:
        raise ValueError("No Specified Basin HUC")
    if end_date is None:
        end_date = "2100-01-01"
        
    gdf = get_stations(hucs= hucs, returnType='gpd')
    
    ds = fetch_snotel(gdf['stationTriplet'].tolist(),elements=elements, duration = duration ,start_date= start_date, end_date=end_date)
    
    n_stations = len(gdf)
    #basin stats 
    # -----
    basin_mean = ds.mean(dim='station')
    basin_median = ds.median(dim = 'station')
    basin_max = ds.max(dim = 'station')
    basin_min = ds.min(dim = 'station')
    
    basin_stats = xr.concat([basin_mean,basin_median,basin_max,basin_min],dim = pd.Index(["MEAN", "MEDIAN", "MAX", "MIN"],dtype="object", name='stat'))
    basin_stats.attrs['hucs'] = hucs
    basin_stats.attrs['Number of Stations'] = n_stations
    basin_stats.attrs['Description'] = f"Basin-averaged statistics for HUC: {hucs}"
    #------
   
    #climatology
    climatology_start = climatology_period[0]
    climatology_end = climatology_period[1]
    climatology_mean = basin_stats.sel(stat='MEAN', time=slice(climatology_start,climatology_end)).groupby("time.month").mean().drop_vars('stat')
    climatology_median = basin_stats.sel(stat='MEDIAN', time=slice(climatology_start,climatology_end)).groupby("time.month").median().drop_vars('stat')

    climatology = xr.concat([climatology_mean, climatology_median],dim = pd.Index(["MEAN","MEDIAN"],dtype="object", name = 'climatology'))
    climatology.attrs['hucs'] = hucs
    climatology.attrs['Number of Stations'] = n_stations
    climatology.attrs['duration'] = "monthly climatology"
    climatology.attrs['Description'] = f"Monthly Climatology for HUC: {hucs}"
    
    
    # Percent of median for WTEQ
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
        # "percent_of_median": percent_of_median,
        "stations": gdf  
    }

    

    
    



if __name__ == "__main__":
    basin_sum = basin_summary(hucs=[160502], 
                             start_date = "2000-10-01",
                             end_date= "2025-10-01", 
                             elements=['WTEQ'],
                             climatology_period=("2000-10-01", "2025-10-01"))

   
    
    basin_stats = basin_sum['basin_stats']
    
    # print(basin_stats['WTEQ'].sel(stat = 'MEAN').values.max())
    # percent_of_median = basin_sum['percent_of_median']
    # print(percent_of_median)
    # basin_stats['WTEQ'].sel(stat = 'MEAN').plot()
    # percent_of_median.plot()
    # print(percent_of_median.max().values)
    # print(percent_of_median.min().values)
    
    plt.show()
    