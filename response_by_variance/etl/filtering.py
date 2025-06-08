"""
Data Filtering Module for Immune Health Response Analysis.

This module provides functionality for filtering and cleaning immune response data.
It handles various types of filtering operations:
1. Basic column filtering (inclusion/exclusion)
2. NaN removal
3. Statistical outlier removal
4. Median and variance threshold filtering

The module uses Polars for efficient data manipulation and filtering operations.
"""

import polars as pl
from typing import Optional, Union

class DataFilter:
    """
    Unified filtering class that handles all data filtering operations.
    
    This class provides a comprehensive interface for filtering immune response data,
    consolidating various filtering operations into a single class. It handles:
    1. Basic column filtering (inclusion/exclusion)
    2. NaN removal
    3. Median/variance thresholds
    4. Statistical outlier removal
    
    The class maintains configuration parameters for different types of filters
    and provides methods to apply them in the correct order.
    
    Attributes:
        median_threshold (float): Minimum median value to include in filtered data
        variance_threshold (float): Minimum variance value to include in filtered data
        std_dev_count (int): Number of standard deviations for outlier detection
    """
    
    def __init__(self, 
                 median_threshold: float = 10.0,
                 variance_threshold: float = 20.0,
                 std_dev_count: int = 3):
        """
        Initialize filter with thresholds and parameters.
        
        Args:
            median_threshold: Minimum median value to include (default: 10.0)
            variance_threshold: Minimum variance value to include (default: 20.0)
            std_dev_count: Number of standard deviations for outlier removal (default: 3)
        """
        self.median_threshold = median_threshold
        self.variance_threshold = variance_threshold
        self.std_dev_count = std_dev_count

    def apply_filters(self, 
                     df: pl.DataFrame,
                     filters: dict[str, str],
                     negate: bool = False,
                     value_column: str = "value") -> pl.DataFrame:
        """
        Apply basic column filters to the dataframe.
        
        This function performs two operations:
        1. Removes rows with NaN values in the specified value column
        2. Applies column-based filters (inclusion or exclusion)
        
        Args:
            df: Input DataFrame to filter
            filters: Dictionary mapping column names to values to filter by
            negate: If True, exclude matching rows instead of including them (default: False)
            value_column: Name of the column containing values to check for NaNs (default: "value")
            
        Returns:
            DataFrame with filters applied and NaNs removed
        """
        # Remove NaNs from value column
        df = df.drop_nans(value_column)
        
        # Apply filters
        for column, value in filters.items():
            if negate:
                df = df.filter(pl.col(column) != value)
            else:
                df = df.filter(pl.col(column) == value)
                
        return df

    def filter_by_median_variance(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Filter data based on median and variance thresholds.
        
        This function removes rows where either:
        1. The median value is less than or equal to median_threshold
        2. The variance value is less than or equal to variance_threshold
        
        Args:
            df: DataFrame containing 'median' and 'variance' columns
            
        Returns:
            DataFrame with rows meeting both threshold criteria
        """
        print(f"Filtering data for median > {self.median_threshold} and variance > {self.variance_threshold}...")
        
        filtered_df = df.filter(
            (pl.col("median") > self.median_threshold) & 
            (pl.col("variance") > self.variance_threshold)
        )
        
        print(f"Filtered data from {len(df)} to {len(filtered_df)} rows")
        return filtered_df

    def remove_outliers(self, 
                       df: pl.DataFrame,
                       by_grouping_columns: list[str],
                       value_column: str = "value") -> pl.DataFrame:
        """
        Remove statistical outliers based on standard deviation.
        
        This function:
        1. Groups data by specified columns
        2. Calculates standard deviation for each group
        3. Removes values that are more than std_dev_count standard deviations
           from the mean in either direction
        
        Args:
            df: Input DataFrame
            by_grouping_columns: Columns to group by for outlier calculation
            value_column: Name of the column containing values to check for outliers (default: "value")
            
        Returns:
            DataFrame with outliers removed
        """
        # Calculate standard deviation for each group
        grouped_std_var = (
            df.group_by(by_grouping_columns)
            .agg(pl.col(value_column).std())
            .rename({value_column: "std"})
        )

        # Join std values and filter outliers
        return df.join(grouped_std_var, how="inner", on=by_grouping_columns).filter(
            (pl.col(value_column) <= pl.col("std") * self.std_dev_count) &
            (pl.col(value_column) >= -pl.col("std") * self.std_dev_count)
        )

    def apply_all_filters(self,
                         df: pl.DataFrame,
                         initial_filters: dict[str, str],
                         grouping_columns: list[str],
                         value_column: str = "value") -> pl.DataFrame:
        """
        Apply all filtering operations in the correct order.
        
        This function applies filters in the following sequence:
        1. Initial column filters and NaN removal
        2. Statistical outlier removal
        3. Median and variance threshold filtering (if applicable)
        
        Args:
            df: Input DataFrame to filter
            initial_filters: Dictionary of column-value pairs for initial filtering
            grouping_columns: Columns to group by for outlier removal
            value_column: Name of the column containing values to filter (default: "value")
            
        Returns:
            DataFrame with all filters applied
        """
        # 1. Apply initial filters and remove NaNs
        df = self.apply_filters(df, initial_filters, value_column=value_column)
        
        # 2. Remove outliers
        df = self.remove_outliers(df, grouping_columns, value_column)
        
        # 3. If median and variance columns exist, apply those filters
        if "median" in df.columns and "variance" in df.columns:
            df = self.filter_by_median_variance(df)
            
        return df