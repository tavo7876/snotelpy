import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import xarray as xr

# Maps element codes to (long name, units) for axis labels
_ELEMENT_META = {
    "WTEQ": ("Snow Water Equivalent", "in"),
    "PREC": ("Precipitation Accumulation", "in"),
    "SNWD": ("Snow Depth", "in"),
    "TAVG": ("Air Temperature Average", "°F"),
    "TMAX": ("Air Temperature Maximum", "°F"),
    "TMIN": ("Air Temperature Minimum", "°F"),
}

def element_timeseries(ds, element="WTEQ", show_plot=True, ax=None, figsize=(10, 4)):
    """
    Plot a time series for a given SNOTEL element across all stations in a dataset.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset returned by fetch_snotel(), with dimensions (time, station).
    element : str, optional
        Element code to plot (e.g. "WTEQ", "SNWD", "TAVG"). Default is "WTEQ".
    show_plot : bool, optional
        If True, calls plt.show() after rendering. Set to False when embedding
        in a subplot or saving programmatically. Default is True.
    ax : matplotlib.axes.Axes, optional
        Axes object to plot onto. If None, a new figure and axes are created.
    figsize : tuple, optional
        Figure size as (width, height) in inches. Only used when ax is None.
        Default is (10, 4).

    Returns
    -------
    matplotlib.axes.Axes
        The axes object, for further customization or embedding in subplots.

    Raises
    ------
    TypeError
        If ds is not an xarray.Dataset.
    ValueError
        If the element code is not found in ds.

    Examples
    --------
    >>> ds = sp.fetch_snotel(stations=["602:CO:SNTL"], elements=["WTEQ"],
    ...                      start_date="2022-10-01", end_date="2023-03-31")
    >>> ax = sp.plot.element_timeseries(ds, element="WTEQ")

    >>> # Embed in a subplot without auto-showing
    >>> fig, axes = plt.subplots(2, 1, figsize=(10, 8))
    >>> sp.plot.element_timeseries(ds, element="WTEQ", show_plot=False, ax=axes[0])
    >>> sp.plot.element_timeseries(ds, element="SNWD", show_plot=False, ax=axes[1])
    >>> plt.tight_layout()
    >>> plt.show()
    """
    # --- Input validation ---
    if not isinstance(ds, xr.Dataset):
        raise TypeError(f"Expected an xarray.Dataset, got {type(ds).__name__}.")

    element = element.strip().upper()

    if element not in ds:
        available = list(ds.data_vars)
        raise ValueError(
            f"Element '{element}' not found in dataset. "
            f"Available variables: {available}"
        )

    # --- Axes setup ---
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    # --- Plotting ---
    stations = ds.station.values
    for station in stations:
        da = ds[element].sel(station=station)
        ax.plot(da.time.values, da.values, label=station, marker="o",
                markersize=2, linewidth=1.2)

    # --- Labels and formatting ---
    long_name, units = _ELEMENT_META.get(element, (element, ""))
    ylabel = f"{long_name} ({units})" if units else long_name

    ax.set_title(f"{long_name} — All Stations", fontsize=10, fontweight="bold")
    ax.set_xlabel("Date", fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.tick_params(axis="both", labelsize=8)

    # Rotate x-axis tick labels
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    # Legend: inside if few stations, outside if many
    if len(stations) <= 6:
        ax.legend(fontsize=7, loc="best")
    else:
        ax.legend(fontsize=6, loc="upper left",
                  bbox_to_anchor=(1.01, 1), borderaxespad=0)

    plt.tight_layout()

    if show_plot:
        plt.show()

    return ax