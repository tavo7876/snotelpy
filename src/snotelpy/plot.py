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
        
    ax.legend()
    ax.grid(True, alpha = 0.3)
    ax.set_title(f"{element} time series")
    ax.set_xlabel('Time')
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    if ShowPlot:
        plt.show() 
        
    return ax
    
  
        
        
        
    