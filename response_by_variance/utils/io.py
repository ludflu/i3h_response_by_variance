"""
Input/Output Utilities for Immune Health Response Analysis.

This module provides functions for handling data input and output operations in the
immune health response analysis pipeline. It focuses on:

1. Results Saving:
   - Saving analysis results to CSV files
   - Proper formatting of numeric values
   - Progress reporting and error handling
   - Directory management

2. Data Formatting:
   - Rounding float values for readability
   - Maintaining data types and precision
   - Handling different column types

3. File Management:
   - Creating output directories
   - Managing file paths
   - Handling file operations safely

The module uses Polars for efficient DataFrame operations and provides
user-friendly progress reporting during file operations.
"""

import os
import polars as pl

def save_results(dataframes: dict[str, pl.DataFrame], output_dir: str) -> None:
    """
    Save analysis results to output files with proper formatting.
    
    This function handles the saving of multiple DataFrames to CSV files with:
    1. Automatic output directory creation
    2. Float value rounding for readability
    3. Progress reporting for each file
    4. Safe file operations
    
    The function processes each DataFrame to:
    - Round float values to 3 decimal places
    - Preserve non-float columns as is
    - Report the size of each saved file
    
    Args:
        dataframes: Dictionary mapping output filenames to DataFrames
                   Example: {'correlations.csv': corr_df, 'metrics.csv': metrics_df}
        output_dir: Directory path where output files will be saved
                   Will be created if it doesn't exist
    
    Example:
        >>> results = {
        ...     'correlations.csv': corr_df,
        ...     'metrics.csv': metrics_df
        ... }
        >>> save_results(results, 'output/analysis')
        Saving correlations.csv...
        Saved to output/analysis/correlations.csv (100 rows, 5 columns)
        Saving metrics.csv...
        Saved to output/analysis/metrics.csv (50 rows, 3 columns)
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save each DataFrame to CSV
    for filename, df in dataframes.items():
        # Round all float columns to 3 decimal places
        float_cols = [col for col in df.columns if df.schema[col] in [pl.Float32, pl.Float64]]
        
        if float_cols:
            # Create expressions for each column, rounding floats
            expr = [
                pl.col(col).round(3) if col in float_cols else pl.col(col)
                for col in df.columns
            ]
            # Apply the expressions to round the float columns
            rounded_df = df.select(expr)
        else:
            rounded_df = df
        
        output_path = os.path.join(output_dir, filename)
        print(f"Saving {filename}...")
        rounded_df.write_csv(output_path)
        print(f"Saved to {output_path} ({df.shape[0]} rows, {df.shape[1]} columns)")