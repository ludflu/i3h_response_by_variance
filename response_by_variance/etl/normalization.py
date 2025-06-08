"""
Data Normalization Module for Immune Health Response Analysis.

This module provides functionality for normalizing and aggregating immune response data.
It handles various operations:
1. Normalization against basal/control conditions
2. Statistical aggregation (median, variance)
3. Population averaging
4. Summary score calculation

The module uses Polars for efficient data manipulation and computation.
"""

import polars as pl
from typing import Optional, Union, List

class DataNormalizer:
    """
    Unified normalization class that handles all data normalization and aggregation operations.
    
    This class provides a comprehensive interface for normalizing and aggregating
    immune response data. It handles:
    1. Basal condition normalization
    2. Statistical aggregation (median, variance)
    3. Population averaging
    4. Summary score calculation
    
    The class maintains configuration parameters for normalization and provides
    methods to perform various normalization and aggregation operations.
    
    Attributes:
        basal_filters (dict): Filters to identify basal/control conditions
        normalization_join (list): Columns to join on for normalization
        value_column (str): Name of the column containing values to normalize
    """
    
    def __init__(self,
                 basal_filters: dict[str, str],
                 normalization_join: list[str],
                 value_column: str = "value"):
        """
        Initialize normalizer with parameters.
        
        Args:
            basal_filters: Dictionary mapping column names to values that identify basal conditions
            normalization_join: List of column names to join on for normalization
            value_column: Name of the column containing values to normalize (default: "value")
        """
        self.basal_filters = basal_filters
        self.normalization_join = normalization_join
        self.value_column = value_column

    def normalize_by_basal(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Normalize values against basal/control conditions.
        
        This function:
        1. Identifies basal conditions using basal_filters
        2. Separates basal and non-basal values
        3. Joins them based on normalization_join columns
        4. Calculates normalized values by subtracting basal values
        
        Args:
            df: Input DataFrame containing both basal and non-basal conditions
            
        Returns:
            DataFrame with normalized values (original value - basal value)
        """
        # Get basal values
        base = (
            df.filter(pl.col("Condition") == list(self.basal_filters.values())[0])
            .rename({self.value_column: "basal_value"})
            .drop("Condition")
        )

        # Get non-basal values
        non_base = df.filter(pl.col("Condition") != list(self.basal_filters.values())[0])

        # Join and calculate normalized values
        return base.join(non_base, how="inner", on=self.normalization_join).with_columns(
            (pl.col(self.value_column) - pl.col("basal_value")).alias("normalized_value")
        )

    def aggregate(self, df: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
        """
        Calculate median and variance for grouped data.
        
        This function:
        1. Groups data by specified columns
        2. Calculates median of normalized values
        3. Calculates variance of normalized values
        4. Joins the results
        
        Args:
            df: Input DataFrame containing normalized values
            group_by: List of column names to group by
            
        Returns:
            DataFrame with median and variance calculations for each group
        """
        # Calculate median
        med = (
            df.group_by(group_by)
            .agg(pl.col("normalized_value").median())
            .rename({"normalized_value": "median"})
        )
        
        # Calculate variance
        var = (
            df.group_by(group_by)
            .agg(pl.col("normalized_value").var())
            .rename({"normalized_value": "variance"})
        )
        
        # Join results
        return med.join(var, how="inner", on=group_by)

    def normalize_and_aggregate(self, df: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
        """
        DEPRECATED: This function will be removed in a future version.
        Use DataProcessor.process_data() with appropriate aggregation parameters instead.
        
        Perform complete normalization and aggregation pipeline.
        
        This function combines two operations:
        1. Normalize values against basal conditions
        2. Aggregate normalized values by specified groups
        
        Args:
            df: Input DataFrame to process
            group_by: List of column names to group by for aggregation
            
        Returns:
            DataFrame with normalized and aggregated values
        """
        normalized = self.normalize_by_basal(df)
        return self.aggregate(normalized, group_by)