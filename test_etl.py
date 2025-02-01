import pytest
import polars as pl
from etl import (
    filter_by_group,
    filter_by_group_negate,
    filter_data,
    remove_outliers,
    normalize_by_basal,
    group_by_and_agg,
    load_data,
    save_data,
)


def test_filter_by_group():
    df = pl.DataFrame({"col1": ["a", "a", "b", "b"], "col2": [1, 2, 3, 4]})
    filtered = filter_by_group(df, {"col1": "a"})
    assert filtered.shape == (2, 2)
    assert filtered["col1"].to_list() == ["a", "a"]


def test_filter_by_group_negate():
    df = pl.DataFrame({"col1": ["a", "a", "b", "b"], "col2": [1, 2, 3, 4]})
    filtered = filter_by_group_negate(df, {"col1": "a"})
    assert filtered.shape == (2, 2)
    assert filtered["col1"].to_list() == ["b", "b"]


def test_filter_data():
    df = pl.DataFrame(
        {"Species": ["Human", "Mouse", "Human"], "value": [1.0, 2.0, None]}
    )
    filtered = filter_data(df, {"Species": "Human"})
    assert filtered.shape == (1, 2)
    assert filtered["Species"].to_list() == ["Human"]
    assert filtered["value"].to_list() == [1.0]


def test_remove_outliers():
    df = pl.DataFrame(
        {"group": ["A", "A", "A", "B", "B"], "value": [1.0, 2.0, 10.0, 1.0, 2.0]}
    )
    cleaned = remove_outliers(df, ["group"], num_std_dev=2)
    assert cleaned.shape == (3, 3)
    assert 10.0 not in cleaned["value"].to_list()


def test_normalize_by_basal():
    df = pl.DataFrame(
        {
            "population": ["p1", "p1", "p1"],
            "reagent": ["r1", "r1", "r1"],
            "Donor": ["d1", "d1", "d1"],
            "Condition": ["Basal", "Test1", "Test2"],
            "value": [1.0, 10.0, 15.0],
        }
    )
    normalized = normalize_by_basal(
        df, {"Condition": "Basal"}, ["population", "reagent", "Donor"]
    )
    assert normalized.shape == (2, 7)
    assert normalized["normalized_value"].to_list() == [9.0, 14.0]


def test_group_by_and_agg():
    df = pl.DataFrame(
        {
            "population": ["p1", "p1", "p2"],
            "reagent": ["r1", "r1", "r2"],
            "Condition": ["c1", "c1", "c2"],
            "normalized_value": [1.0, 2.0, 3.0],
        }
    )
    grouped = group_by_and_agg(df, ["population", "reagent", "Condition"])
    assert grouped.shape == (2, 5)
    assert "median" in grouped.columns
    assert "variance" in grouped.columns


def test_load_and_save_data(tmp_path):
    # Create test data
    df = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Save test data
    test_file = tmp_path / "test.csv"
    save_data(df, str(test_file))

    # Load and verify
    loaded_df = load_data(str(test_file))
    assert loaded_df.shape == df.shape
    assert loaded_df.columns == df.columns
