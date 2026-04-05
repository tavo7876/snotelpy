import matplotlib.pyplot as plt
import xarray as xr



def element_timeseries(ds, element = "WTEQ", ShowPlot = True):
    '''
    ------
    enter a ds
    '''

    fig, ax = plt.subplots()
    for station in ds.station.values:
        ds[element.strip().upper()].sel(station=station).plot(ax=ax,label=station,marker = 'o')
        
    ax.legend()
    ax.grid(True, alpha = 0.3)
    ax.set_title(f"{element} time series")
    ax.set_xlabel('Time')
    plt.tight_layout()
    if ShowPlot:
        plt.show() 
        
    return ax
    
  
        
        
        
    