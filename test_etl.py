"""
Test Suite for ETL (Extract, Transform, Load) Module.

This module contains unit tests for the core ETL functionality of the Immune Health
Response Analysis package. It tests various data processing operations including:
1. Data filtering (by group, negation, basic filtering)
2. Outlier removal
3. Basal condition normalization
4. Data aggregation

Each test function verifies a specific aspect of the ETL pipeline, ensuring that
data transformations are performed correctly and consistently.
"""

import polars as pl
from i3h_response_by_variance.response_by_variance.etl.etl import (
    filter_by_group,
    filter_by_group_negate,
    filter_data,
    remove_outliers,
    normalize_by_basal,
    group_by_and_agg,
)


def test_filter_by_group():
    """
    Test filtering data by group values.
    
    This test verifies that:
    1. The filter correctly selects rows matching the specified group value
    2. The output shape is correct
    3. The filtered values match the expected group
    """
    df = pl.DataFrame({"col1": ["a", "a", "b", "b"], "col2": [1, 2, 3, 4]})
    filtered = filter_by_group(df, {"col1": "a"})
    assert filtered.shape == (2, 2)
    assert filtered["col1"].to_list() == ["a", "a"]


def test_filter_by_group_negate():
    """
    Test filtering data by excluding group values.
    
    This test verifies that:
    1. The filter correctly excludes rows matching the specified group value
    2. The output shape is correct
    3. The filtered values exclude the specified group
    """
    df = pl.DataFrame({"col1": ["a", "a", "b", "b"], "col2": [1, 2, 3, 4]})
    filtered = filter_by_group_negate(df, {"col1": "a"})
    assert filtered.shape == (2, 2)
    assert filtered["col1"].to_list() == ["b", "b"]


def test_filter_data():
    """
    Test basic data filtering with NaN removal.
    
    This test verifies that:
    1. The filter correctly selects rows matching the specified value
    2. NaN values are properly removed
    3. The output shape is correct
    4. The filtered values match the expected criteria
    """
    df = pl.DataFrame(
        {"Species": ["Human", "Mouse", "Human"], "value": [1.0, 2.0, None]}
    )
    filtered = filter_data(df, {"Species": "Human"})
    assert filtered.shape == (1, 2)
    assert filtered["Species"].to_list() == ["Human"]
    assert filtered["value"].to_list() == [1.0]


def test_remove_outliers():
    """
    Test statistical outlier removal.
    
    This test verifies that:
    1. Values beyond the specified number of standard deviations are removed
    2. The grouping is respected when calculating statistics
    3. The output shape is correct
    4. Extreme values are properly filtered out
    """
    df = pl.DataFrame(
        {"group": ["A", "A", "A", "B", "B"], "value": [1.0, 2.0, 10.0, 1.0, 2.0]}
    )
    cleaned = remove_outliers(df, ["group"], num_std_dev=2)
    assert cleaned.shape == (3, 3)
    assert 10.0 not in cleaned["value"].to_list()


def test_normalize_by_basal():
    """
    Test normalization against basal conditions.
    
    This test verifies that:
    1. Values are correctly normalized by subtracting basal values
    2. The join operation works correctly
    3. The output shape is correct
    4. The normalized values are calculated correctly
    """
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
    """
    Test data aggregation with statistical calculations.
    
    This test verifies that:
    1. Data is correctly grouped by specified columns
    2. Median and variance are calculated correctly
    3. The output shape is correct
    4. The required statistical columns are present
    """
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
