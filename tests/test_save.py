# save_data — no API, no I/O beyond tmp_path
import pytest
import numpy as np
import pandas as pd
import xarray as xr
from snotelpy.fetch import save_data


# ---------------------------------------------------------------------------
# pytest's built-in tmp_path fixture gives you a temporary directory that is
# automatically cleaned up after each test — safe for writing real files
# ---------------------------------------------------------------------------


# ============================================================
# xr.Dataset  →  .nc
# ============================================================

def test_dataset_creates_nc_file(tmp_path):
    ds = xr.Dataset({"WTEQ": (["time", "station"], np.zeros((3, 1)))})
    outfile = tmp_path / "test_output"
    save_data(ds, filename=str(outfile))
    assert (tmp_path / "test_output.nc").exists()


def test_dataset_saved_nc_is_readable(tmp_path):
    ds = xr.Dataset({"WTEQ": (["time", "station"], np.zeros((3, 1)))})
    outfile = tmp_path / "test_output"
    save_data(ds, filename=str(outfile))
    loaded = xr.open_dataset(str(outfile) + ".nc")
    assert "WTEQ" in loaded.data_vars



def test_dataset_default_filename(tmp_path, monkeypatch):
    ds = xr.Dataset({"WTEQ": (["time", "station"], np.zeros((3, 1)))})
    monkeypatch.chdir(tmp_path)
    save_data(ds)
    assert (tmp_path / "snotel_data.nc").exists()


# ============================================================
# pd.DataFrame  →  .csv
# ============================================================

def test_dataframe_creates_csv_file(tmp_path):
    df = pd.DataFrame({"station": ["602:CO:SNTL"], "value": [10.7]})
    outfile = tmp_path / "test_output"
    save_data(df, filename=str(outfile))
    assert (tmp_path / "test_output.csv").exists()


def test_dataframe_saved_csv_is_readable(tmp_path):
    df = pd.DataFrame({"station": ["602:CO:SNTL"], "value": [10.7]})
    outfile = tmp_path / "test_output"
    save_data(df, filename=str(outfile))
    loaded = pd.read_csv(str(outfile) + ".csv")
    for col in ["station", "value"]:
        assert col in loaded.columns
    


# ============================================================
# TypeError for unsupported input types
# ============================================================

def test_wrong_type_string_raises_type_error():
    with pytest.raises(TypeError):
        save_data("not a dataset")


def test_wrong_type_list_raises_type_error():
    with pytest.raises(TypeError):
        save_data([1, 2, 3])


def test_wrong_type_int_raises_type_error():
    with pytest.raises(TypeError):
        save_data(42)
