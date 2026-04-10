import matplotlib.pyplot as plt
import xarray as xr



def element_timeseries(ds, element = "WTEQ", ShowPlot = True, ax=None):
    '''
    ------
    enter a ds
    '''

    if ax is None:
        fig, ax = plt.subplots()
    for station in ds.station.values:
        ds[element.strip().upper()].sel(station=station).plot(ax=ax,label=station,marker = 'o')
        
    # ax.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=7)
    ax.grid(True, alpha = 0.3)
    ax.set_title(f"{element} time series", fontsize = 9)
    # ax.set_xlabel('Time')
    plt.xticks(rotation=45)
    
    if ShowPlot:
        plt.show() 
        
    return ax
    
  
        
        
        
    