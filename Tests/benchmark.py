"""
Benchmark: sequential vs parallel chunked fetching of SNOTEL data.

Run with:
    python benchmark.py
"""

import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import xarray as xr

from snotelpy.fetch import _fetch_data, _chunkgen


# ---- Request that forces multiple chunks ----
# Sized to ~80k estimated points: above the 50k threshold (so the new logic splits into 8 chunks) but small enough to
# finish quickly. 1 station x 1 element x hourly x ~9 years ~= 78,800 points.
STATIONS   = ["663:CO:SNTL"]
ELEMENTS   = ["WTEQ"]
DURATION   = "HOURLY"
START_DATE = "2016-01-01"
END_DATE   = "2025-01-01"


def fetch_sequential(stations, elements, duration, start_date, end_date):
    """Original behavior: one chunk at a time, in a for loop."""
    n_chunks, est = _chunkgen(
        duration=duration, stations=stations, elements=elements,
        start_date=start_date, end_date=end_date,
    )
    print(f"  estimated points: {est:,}  ->  {n_chunks} chunk(s)")

    bounds = pd.date_range(
        start=start_date,
        end=min(pd.Timestamp(end_date), pd.Timestamp.today()),
        periods=n_chunks + 1,
    )
    chunks = []
    for i in range(n_chunks):
        chunk_start = bounds[i].strftime("%Y-%m-%d")
        chunk_end = bounds[i + 1].strftime("%Y-%m-%d")
        chunk_ds = _fetch_data(
            stations=stations,
            elements=elements,
            duration=duration,
            start_date=chunk_start,
            end_date=chunk_end,
        )
        chunks.append(chunk_ds)
    ds = xr.concat(chunks, dim="time", join="outer")
    ds = ds.isel(time=~pd.DatetimeIndex(ds.time.values).duplicated())
    return ds


def fetch_parallel(stations, elements, duration, start_date, end_date,
                   max_workers=8):
    """Parallel version: all chunks dispatched concurrently via threads."""
    n_chunks, est = _chunkgen(
        duration=duration, stations=stations, elements=elements,
        start_date=start_date, end_date=end_date,
    )
    print(f"  estimated points: {est:,}  ->  {n_chunks} chunk(s), "
          f"{min(n_chunks, max_workers)} workers")

    bounds = pd.date_range(
        start=start_date,
        end=min(pd.Timestamp(end_date), pd.Timestamp.today()),
        periods=n_chunks + 1,
    )
    chunks = [None] * n_chunks
    workers = min(n_chunks, max_workers)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_idx = {}
        for i in range(n_chunks):
            chunk_start = bounds[i].strftime("%Y-%m-%d")
            chunk_end = bounds[i + 1].strftime("%Y-%m-%d")
            fut = ex.submit(
                _fetch_data, stations, elements, duration,
                chunk_start, chunk_end,
            )
            future_to_idx[fut] = i
        for fut in as_completed(future_to_idx):
            chunks[future_to_idx[fut]] = fut.result()
    ds = xr.concat(chunks, dim="time", join="outer")
    ds = ds.isel(time=~pd.DatetimeIndex(ds.time.values).duplicated())
    return ds


def time_it(label, fn, *args, **kwargs):
    print(f"\n[{label}]")
    t0 = time.perf_counter()
    ds = fn(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    print(f"  finished in {elapsed:.2f}s  ({len(ds.time)} time steps, "
          f"{len(ds.station)} stations, {len(ds.data_vars)} variables)")
    return elapsed, ds


def main():
    print("=" * 60)
    print("SNOTEL fetch benchmark")
    print("=" * 60)
    print(f"stations:   {STATIONS}")
    print(f"elements:   {ELEMENTS}")
    print(f"duration:   {DURATION}")
    print(f"date range: {START_DATE}  ->  {END_DATE}")

    t_seq, _ = time_it("sequential", fetch_sequential,
                       STATIONS, ELEMENTS, DURATION, START_DATE, END_DATE)
    t_par, _ = time_it("parallel  ", fetch_parallel,
                       STATIONS, ELEMENTS, DURATION, START_DATE, END_DATE)

    print("\n" + "=" * 60)
    print(f"sequential: {t_seq:6.2f}s")
    print(f"parallel:   {t_par:6.2f}s")
    if t_par > 0:
        print(f"speedup:    {t_seq / t_par:6.2f}x")
    print("=" * 60)


if __name__ == "__main__":
    main()