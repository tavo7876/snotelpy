from snotelpy.fetch import fetch_data, get_stations
from snotelpy import plot
import matplotlib.pyplot as plt




def basin_summary(hucs = [], duration = "MONTHLY", start_date = "1991-10-01", end_date = ""):
    '''
    
    '''
    elements = "WTEQ, PREC, SNWD, TAVG, TMAX, TMIN"
    gdf = get_stations(hucs= hucs, returnType='gpd')
    ds = fetch_data(gdf['stationTriplet'].tolist(),elements=elements, duration = duration ,start_date= start_date, end_date=end_date)
    print(ds)
    fig, axes = plt.subplots(3,2, figsize = (16,14))
    plt.tight_layout(pad=5.0,h_pad=3.0)
    
    plot.element_timeseries(ds,"WTEQ", False,ax= axes[0, 0])
    plot.element_timeseries(ds,"PREC",False,ax = axes[0, 1])
    plot.element_timeseries(ds,"SNWD", False,ax= axes[1, 0])
    plot.element_timeseries(ds,"TAVG",False,ax = axes[1, 1])
    plot.element_timeseries(ds,"TMAX", False,ax= axes[2, 0])
    plot.element_timeseries(ds,"TMIN",False,ax = axes[2, 1])
    
    plt.show()
    
    



if __name__ == "__main__":
    basin_summary(hucs = ['10190005'],start_date = "2000-01-01", end_date = "2025-01-01")
    