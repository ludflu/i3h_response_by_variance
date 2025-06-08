"""
Pivot Table Generation for Immune Health Response Analysis.

This module provides functions for creating pivot tables that will be used to generate correlation matrices:
1. Reagent-by-reagent correlations within each (population, condition) pair
2. Population-by-population correlations for each (reagent, condition) pair
3. Condition-by-condition correlations for each (population, reagent) pair

The pivot tables are designed to facilitate:
- Pattern recognition in immune responses
- Comparison of different experimental conditions
- Analysis of variance across populations
"""

import polars as pl
from typing import Dict, Tuple

def create_population_reagent_pivot(df: pl.DataFrame) -> Dict[Tuple[str, str], pl.DataFrame]:
    """
    Create pivot tables for reagent-by-reagent correlations within each (population, condition) pair.
    
    This function creates pivot tables that show:
    - Rows: Donors
    - Columns: Reagents
    - Values: Response values
    
    One pivot table is created for each unique (population, condition) pair.
    These will be used to compute reagent-by-reagent correlations.
    
    Args:
        df: DataFrame containing population, reagent, Condition, and value columns
        
    Returns:
        Dictionary mapping (population, condition) tuples to pivot tables
    """
    print("Creating population by reagent pivot tables...")
    
    # Ensure we have the required columns with correct types
    required_cols = ["population", "reagent", "Condition", "median"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in DataFrame")
    
    # Ensure median column is numeric
    if not df.schema["median"].is_numeric():
        raise ValueError("'median' column must be numeric")
    
    results = {}
    
    # Group by population and condition
    for (pop, cond), group in df.group_by(["population", "Condition"]):
        # Create pivot table for this group
        # Each row is a unique (population, condition) group
        pivot_df = group.pivot(
            values="median",
            columns="reagent",
            aggregate_function="median"  # Use median for consistency
        )
        results[(pop, cond)] = pivot_df
    
    print(f"Created {len(results)} population by reagent pivot tables")
    return results

def create_population_condition_pivot(df: pl.DataFrame) -> Dict[Tuple[str, str], pl.DataFrame]:
    """
    Create pivot tables for population-by-population correlations for each (reagent, condition) pair.
    
    This function creates pivot tables that show:
    - Rows: Donors
    - Columns: Populations
    - Values: Response values
    
    One pivot table is created for each unique (reagent, condition) pair.
    These will be used to compute population-by-population correlations.
    
    Args:
        df: DataFrame containing population, reagent, Condition, and value columns
        
    Returns:
        Dictionary mapping (reagent, condition) tuples to pivot tables
    """
    print("Creating population by condition pivot tables...")
    
    # Ensure we have the required columns with correct types
    required_cols = ["population", "reagent", "Condition", "median"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in DataFrame")
    
    # Ensure median column is numeric
    if not df.schema["median"].is_numeric():
        raise ValueError("'median' column must be numeric")
    
    results = {}
    
    # Group by reagent and condition
    for (reag, cond), group in df.group_by(["reagent", "Condition"]):
        # Create pivot table for this group
        pivot_df = group.pivot(
            values="median",
            columns="population",
            aggregate_function="median"  # Use median for consistency
        )
        results[(reag, cond)] = pivot_df
    
    print(f"Created {len(results)} population by condition pivot tables")
    return results

def create_reagent_condition_population_pivot(df: pl.DataFrame) -> Dict[Tuple[str, str], pl.DataFrame]:
    """
    Create pivot tables for condition-by-condition correlations for each (population, reagent) pair.
    
    This function creates pivot tables that show:
    - Rows: Donors
    - Columns: Conditions
    - Values: Response values
    
    One pivot table is created for each unique (population, reagent) pair.
    These will be used to compute condition-by-condition correlations.
    
    Args:
        df: DataFrame containing population, reagent, Condition, and value columns
        
    Returns:
        Dictionary mapping (population, reagent) tuples to pivot tables
    """
    print("Creating reagent condition population pivot tables...")
    
    # Ensure we have the required columns with correct types
    required_cols = ["population", "reagent", "Condition", "median"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in DataFrame")
    
    # Ensure median column is numeric
    if not df.schema["median"].is_numeric():
        raise ValueError("'median' column must be numeric")
    
    results = {}
    
    # Group by population and reagent
    for (pop, reag), group in df.group_by(["population", "reagent"]):
        # Create pivot table for this group
        pivot_df = group.pivot(
            values="median",
            columns="Condition",
            aggregate_function="median"  # Use median for consistency
        )
        results[(pop, reag)] = pivot_df
    
    print(f"Created {len(results)} reagent condition population pivot tables")
    return results

def create_variance_pivot(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create a pivot table showing variance patterns across populations and reagents.
    
    This function creates a pivot table that shows:
    - Rows: Populations
    - Columns: Reagents
    - Values: Variance values
    
    The table helps visualize the variability in responses across different
    population-reagent combinations.
    
    Args:
        df: DataFrame containing population, reagent, and variance columns
        
    Returns:
        DataFrame in pivot format with populations as rows, reagents as columns,
        and variance values as the data
    """
    print("Creating variance pivot table...")
    
    # Ensure we have the required columns with correct types
    required_cols = ["population", "reagent", "variance"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in DataFrame")
    
    # Ensure variance column is numeric
    if not df.schema["variance"].is_numeric():
        raise ValueError("'variance' column must be numeric")
    
    # Create the pivot table
    pivot_df = df.pivot(
        index="population",
        columns="reagent",
        values="variance",
        aggregate_function="median"  # Use median for consistency
    )
    
    print("Created variance pivot table")
    return pivot_df
