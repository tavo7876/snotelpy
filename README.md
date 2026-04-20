# snotelpy

Snotelpy is a lightweight Python Package interface for accessing USDA SNOTEL station data, allowing researchers to quickly retrieve, filter, and analyze snowpack variables across all SNOTEL stations and watersheds via the NRCS AWDB RESTful API.

---

## Motivation
I built this package as there is currently no activley maintained, modern Python package built on the newer [NRCS AWDB RESTful API](https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html). The pervious WSDL/SOAP API used to request SNOTEL data is being trasitioned away to the newer AWDB RESTful API.

`snotelpy` fills this gap - giving researchers a simple, Python interface to fetch, filter, and analze snowpack data from SNOTEL statiosn without manually constructing API URLs or cleaning raw JSNO responses. 

---

## Installation
```bash
git clone https://github.com//snotelpy.git
cd snotelpy
pip install -e .

```

---

## Quick Start
```python
import snotelpy as sp 

#Fetch daily SWE and precipitation for two Colorado stations (2020 water year)
ds = sp.fetch_snotel(
    stations=["602:CO:SNTL", "913:CO:SNTL"],
    elements=["WTEQ", "PREC"],
    duration="DAILY",
    start_date="2019-10-01",
    end_date="2020-09-30"
)

# Print resulting xarray Dataset with (time, stations) dimensions
print(ds)


```
## Functions

### fetch_snotel(stations, elements, duration, start_date, end_date, include_coords)
Fetches one or more SNOTEL variables across one or more stations and returns
an `xarray.Dataset` with `(time, station)` dimensions. Automatically chunks large
requests that exceed the API's 500,000-point limit.


| Parameter | Type | Default | Description |
|---|---|---|---|
| `stations` | list | required | Station triplets, e.g. `["602:CO:SNTL"]`. Supports `*` wildcard. |
| `elements` | list | required | Element codes, e.g. `["WTEQ", "PREC"]`. |
| `duration` | str | `"DAILY"` | `"DAILY"`, `"HOURLY"`, `"SEMIMONTHLY"`, `"MONTHLY"`, `"CALENDAR_YEAR"`, `"WATER_YEAR"` |
| `start_date` | str | `"1991-01-01"` | Begin date in `YYYY-MM-DD` format. |
| `end_date` | str | `"2100-01-01"` | End date in `YYYY-MM-DD` format. |
| `include_coords` | bool | `False` | If `True`, attach latitude, longitude, and elevation as dataset coordinates. |

---

### get_stations(station_triplets, elements, hucs, county_name, station_name, returnStationElements, returnType)
Queries the AWDB API for stations matching the given filters and returns
a `pandas.DataFrame` or a `geopandas.GeoDataFrame` (EPSG:4326).
 Parameter | Type | Default | Description |
|---|---|---|---|
| `station_triplets` | list | `["::SNTL"]` | All SNOTEL stations by default. |
| `elements` | list | `[]` | Filter by element codes, e.g. `["WTEQ"]`. |
| `hucs` | list | `[]` | Filter by HUC watershed codes, e.g. `["10"]`. |
| `county_name` | str | `""` | Filter by county name, e.g. `"Boulder"`. |
| `station_name` | str | `""` | Filter by station name. |
| `returnType` | str | `"pd"` | `"pd"` for pandas DataFrame, `"gpd"` for GeoDataFrame. |


---

### plot.element_timeseries(ds, element, ShowPlot, ax)
Plots a time series for all stations in a dataset for a given element. Returns
the matplotlib `Axes` object for further customization or embedding in subplots.

---

## Common Element Codes

| Code | Description | Units |
|---|---|---|
| `WTEQ` | Snow Water Equivalent | in |
| `PREC` | Precipitation Accumulation | in |
| `SNWD` | Snow Depth | in |
| `TAVG` | Air Temperature Average | °F |
| `TMAX` | Air Temperature Maximum | °F |
| `TMIN` | Air Temperature Minimum | °F |

See `ELEMENTS.yaml` for all elements supported in the NRCS networks, note -- SNOTEL dosent support most of these elements. 

---

## Examples
See the [Examples notebook](Examples.ipynb) for complete workflows including:
- Single-station SWE time series
- Multi-station HUC-filtered requests
- Elevation vs. SWE scatter plots
- GeoDataFrame mapping

---

## Dependencies

- `numpy >= 1.22`
- `matplotlib >= 3.5`
- `requests >= 2.31`
- `pandas >= 2.0`
- `xarray >= 2023.1`
- `pyyaml >= 6.0`
- `geopandas >= 1.0.0`
- `shapely >= 2.0.0`
  
---

## Data Source

Data is sourced from the
[USDA NRCS Air-Water Database (AWDB) REST API](https://wcc.sc.egov.usda.gov/awdbRestApi/swagger-ui/index.html).

---

## Planned Extensions

- SCAN (Soil Climate Analysis Network) support

---

## License

MIT License — see [LICENSE.txt](LICENSE.txt)