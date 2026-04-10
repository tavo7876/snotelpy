from snotelpy.fetch import fetch_data, get_stations
from snotelpy import plot
import matplotlib.pyplot as plt
import contextily as ctx



def basin_summary(hucs = [], duration = "MONTHLY", start_date = "1991-10-01", end_date = ""):
    '''
    
    '''
    elements = "WTEQ, PREC, SNWD, TAVG, TMAX, TMIN"
    gdf = get_stations(hucs= hucs, returnType='gpd')
    ds = fetch_data(gdf['stationTriplet'].tolist(),elements=elements, duration = duration ,start_date= start_date, end_date=end_date)
    print(ds)
    fig, axes = plt.subplots(4,2, figsize = (16,14))
    plot.element_timeseries(ds,"WTEQ", False,ax= axes[0, 0])
    plot.element_timeseries(ds,"PREC",False,ax = axes[0, 1])
    plot.element_timeseries(ds,"SNWD", False,ax= axes[1, 0])
    plot.element_timeseries(ds,"TAVG",False,ax = axes[1, 1])
    plot.element_timeseries(ds,"TMAX", False,ax= axes[2, 0])
    plot.element_timeseries(ds,"TMIN",False,ax = axes[2, 1])
     
    # map
    # gdf_web = gdf.to_crs(epsg=3857)
    # gdf_web.plot(ax=axes[3, 0], column='elevation', cmap='terrain', markersize=50)
    # ctx.add_basemap(axes[3, 0])
    axes[3, 0].axis('off')

    # legend
    handles, labels = axes[0, 0].get_legend_handles_labels()
    axes[3, 1].legend(handles, labels, loc='center',fontsize=7, ncol=4)
    axes[3, 1].axis('off')
    
    plt.tight_layout(pad = 2.0 ,h_pad= 1.0, w_pad= 1.0)
    plt.show()
    

    
    



if __name__ == "__main__":
    basin_summary(hucs = ['1019'],start_date = "2025-01-01", end_date = "")
    