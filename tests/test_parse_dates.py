# pure unit tests — no API, no I/O
import pytest
import pandas as pd
from snotelpy.fetch import _parse_dates


# ============================================================
# DAILY
# ============================================================


def test_daily_returns_datetimeindex():
    values = [
        {"date": "2026-03-25", "value": 10.7},
        {"date": "2026-03-26", "value": 10.8},
    ]
    result = _parse_dates(values, "DAILY")
    assert isinstance(result, pd.DatetimeIndex)



def test_daily_index_is_named_time():
    values = [{"date": "2026-03-25", "value": 10.7}]
    result = _parse_dates(values, "DAILY")
    assert result.name == "time"


def test_daily_correct_length():
    values = [
        {"date": "2026-03-25", "value": 10.7},
        {"date": "2026-03-26", "value": 10.8},
        {"date": "2026-03-27", "value": 10.9},
    ]
    result = _parse_dates(values, "DAILY")
    assert len(values) == len(result)

def test_daily_first_date_is_correct():
    values = [
        {"date": "2026-03-25", "value": 10.7},
        {"date": "2026-03-26", "value": 10.8},
    ]
    result = _parse_dates(values, "DAILY")
    assert result[0] == pd.Timestamp("2026-03-25")


# ============================================================
# HOURLY  (same 'date' key as DAILY — different duration string)
# ============================================================


def test_hourly_returns_datetimeindex():
    values = [
        {"date": "2026-03-25T00:00:00", "value": 5.0},
        {"date": "2026-03-25T01:00:00", "value": 5.1},
    ]
    result = _parse_dates(values, "HOURLY")
    assert isinstance(result, pd.DatetimeIndex)


def test_hourly_index_is_named_time():
    values = [{"date": "2026-03-25T00:00:00", "value": 5.0}]
    result = _parse_dates(values, "HOURLY")
    assert result.name == "time"


# ============================================================
# SEMIMONTHLY  (uses 'collectionDate' key instead of 'date')
# ============================================================

def test_semimonthly_returns_datetimeindex():
    values = [
        {"collectionDate": "2026-03-01", "value": 4.0},
        {"collectionDate": "2026-03-15", "value": 4.2},
    ]
    result = _parse_dates(values, "SEMIMONTHLY")
    assert isinstance(result, pd.DatetimeIndex)


def test_semimonthly_correct_length():
    values = [
        {"collectionDate": "2026-03-01", "value": 4.0},
        {"collectionDate": "2026-03-15", "value": 4.2},
    ]
    result = _parse_dates(values, "SEMIMONTHLY")
    assert len(result) == len(values)


# ============================================================
# MONTHLY
# ============================================================

def test_monthly_returns_datetimeindex():
    values = [{"year": 2026, "month": 3, "value": 10.7}]
    result = _parse_dates(values, "MONTHLY")
    assert isinstance(result, pd.DatetimeIndex)


def test_monthly_month_is_correct():
    values = [{"year": 2026, "month": 3, "value": 10.7}]
    result = _parse_dates(values, "MONTHLY")
    assert result[0].month == 3

def test_monthly_day_is_always_first():
    values = [{"year": 2026, "month": 3, "value": 10.7}]
    result = _parse_dates(values, "MONTHLY")
    assert result[0].day == 1


def test_monthly_correct_length():
    values = [
        {"year": 2026, "month": 1, "value": 5.0},
        {"year": 2026, "month": 2, "value": 6.0},
        {"year": 2026, "month": 3, "value": 7.0},
    ]
    result = _parse_dates(values, "MONTHLY")
    assert len(values) == len(result)


# ============================================================
# CALENDAR_YEAR
# ============================================================

def test_calendar_year_month_is_january():
    values = [{"year": 2026, "value": 10.7}]
    result = _parse_dates(values, "CALENDAR_YEAR")
    assert result[0].month == 1

def test_calendar_year_day_is_first():
    values = [{"year": 2026, "value": 10.7}]
    result = _parse_dates(values, "CALENDAR_YEAR")
    assert result[0].day == 1
    
def test_calendar_year_year_is_correct():
    values = [{"year": 2026, "value": 10.7}]
    result = _parse_dates(values, "CALENDAR_YEAR")
    assert result[0].year == 2026


# ============================================================
# WATER_YEAR
# ============================================================

def test_water_year_month_is_october():
    values = [{"year": 2026, "value": 10.7}]
    result = _parse_dates(values, "WATER_YEAR")
    assert result[0].month == 10


def test_water_year_day_is_first():
    values = [{"year": 2026, "value": 10.7}]
    result = _parse_dates(values, "WATER_YEAR")
    assert result[0].day == 1

def test_water_year_year_is_correct():
    values = [{"year": 2026, "value": 10.7}]
    result = _parse_dates(values, "CALENDAR_YEAR")
    assert result[0].year == 2026

# ============================================================
# Case insensitivity
# ============================================================

def test_lowercase_duration_accepted():
    values = [{"date": "2026-03-25", "value": 10.7}]
    result = _parse_dates(values, "daily")
    assert result[0] == pd.Timestamp("2026-03-25")
