"""
Data Processing Module for Immune Health Response Analysis.

This module provides functions for:
1. Basic data processing and cleaning
2. Data aggregation
3. Data filtering
4. Variance index calculation
"""

import polars as pl
from pathlib import Path

def process_data(input_dir: str) -> pl.DataFrame:
    """
    Process input data through the basic pipeline.
    
    Args:
        input_dir: Directory containing input data files
        
    Returns:
        DataFrame containing processed data
    """
    print("Processing input data...")
    
    # Read input data
    input_path = Path(input_dir) / "human.csv"
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    # Read CSV file
    df = pl.read_csv(input_path)
    
    # Basic cleaning
    df = df.filter(pl.col("Species") == "Human")  # Filter for human data only
    
    print(f"Processed data shape: {df.shape}")
    return df

def aggregate_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate data by population, reagent, and condition.
    
    Args:
        df: DataFrame containing processed data
        
    Returns:
        DataFrame with aggregated statistics
    """
    print("Aggregating data...")
    
    # Group by key columns and calculate statistics
    aggregated = df.group_by(["population", "reagent", "Condition"]).agg([
        pl.col("value").median().alias("median"),
        pl.col("value").var().alias("variance")
    ])
    
    print(f"Aggregated data shape: {aggregated.shape}")
    return aggregated

def filter_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filter data based on quality criteria.
    
    Args:
        df: DataFrame containing aggregated data
        
    Returns:
        DataFrame with filtered data
    """
    print("Filtering data...")
    
    # Remove rows with null values
    filtered = df.filter(~pl.col("median").is_null())
    
    # Remove rows with zero variance
    filtered = filtered.filter(pl.col("variance") > 0)
    
    print(f"Filtered data shape: {filtered.shape}")
    return filtered

def calculate_variance_index(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate variance index for each population-reagent pair.
    
    Args:
        df: DataFrame containing filtered data
        
    Returns:
        DataFrame with variance indices
    """
    print("Calculating variance index...")
    
    # Group by population and reagent
    variance_index = df.group_by(["population", "reagent"]).agg([
        pl.col("variance").mean().alias("variance_index")
    ])
    
    print(f"Variance index shape: {variance_index.shape}")
    return variance_index

