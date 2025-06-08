"""
Correlation Analysis Module for Immune Health Response Data.

This module provides functions for computing and analyzing correlations between:
1. Reagents within each (population, condition) pair
2. Populations for each (reagent, condition) pair
3. Conditions for each (population, reagent) pair

The module includes functions for:
- Computing correlation matrices
- Analyzing correlation patterns
- Generating statistics and visualizations
"""

import os
import numpy as np
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple
from pathlib import Path


def compute_reagent_correlations(pivot_tables: Dict[Tuple[str, str], pl.DataFrame]) -> Dict[Tuple[str, str], pl.DataFrame]:
    """
    Compute reagent-by-reagent correlations within each (population, condition) pair.
    
    Args:
        pivot_tables: Dictionary mapping (population, condition) tuples to pivot tables
                     where rows are donors and columns are reagents
        
    Returns:
        Dictionary mapping (population, condition) tuples to correlation matrices
    """
    print("Computing reagent-by-reagent correlations...")
    
    results = {}
    for (pop, cond), pivot_df in pivot_tables.items():
        print(f"Pivot DataFrame for ({pop}, {cond}) columns: {pivot_df.columns}")
        # Select only meaningful numeric columns (reagent columns and median)
        meaningful_cols = [col for col in pivot_df.columns if col not in ["population", "Condition"] and (pivot_df.schema[col] == pl.Float64 or pivot_df.schema[col] == pl.Int64)]
        print(f"Selected meaningful columns: {meaningful_cols}")
        data = pivot_df.select(meaningful_cols).to_numpy()
        
        # Compute correlation matrix
        corr_matrix = np.corrcoef(data.T)
        
        # Create DataFrame with meaningful column names
        meaningful_names = meaningful_cols
        corr_df = pl.DataFrame(
            {name: corr_matrix[i] for i, name in enumerate(meaningful_names)}
        ).with_columns(pl.Series(name="reagent", values=meaningful_names))
        
        results[(pop, cond)] = corr_df
    
    print(f"Computed correlations for {len(results)} (population, condition) pairs")
    return results


def compute_population_correlations(pivot_tables: Dict[Tuple[str, str], pl.DataFrame]) -> Dict[Tuple[str, str], pl.DataFrame]:
    """
    Compute population-by-population correlations for each (reagent, condition) pair.
    
    Args:
        pivot_tables: Dictionary mapping (reagent, condition) tuples to pivot tables
                     where rows are donors and columns are populations
        
    Returns:
        Dictionary mapping (reagent, condition) tuples to correlation matrices
    """
    print("Computing population-by-population correlations...")
    
    results = {}
    for (reag, cond), pivot_df in pivot_tables.items():
        print(f"Pivot DataFrame for ({reag}, {cond}) columns: {pivot_df.columns}")
        # Select only meaningful numeric columns (population columns and median)
        meaningful_cols = [col for col in pivot_df.columns if col not in ["reagent", "Condition"] and (pivot_df.schema[col] == pl.Float64 or pivot_df.schema[col] == pl.Int64)]
        print(f"Selected meaningful columns: {meaningful_cols}")
        data = pivot_df.select(meaningful_cols).to_numpy()
        
        # Compute correlation matrix
        corr_matrix = np.corrcoef(data.T)
        
        # Create DataFrame with meaningful column names
        meaningful_names = meaningful_cols
        corr_df = pl.DataFrame(
            {name: corr_matrix[i] for i, name in enumerate(meaningful_names)}
        ).with_columns(pl.Series(name="population", values=meaningful_names))
        
        results[(reag, cond)] = corr_df
    
    print(f"Computed correlations for {len(results)} (reagent, condition) pairs")
    return results


def compute_condition_correlations(pivot_tables: Dict[Tuple[str, str], pl.DataFrame]) -> Dict[Tuple[str, str], pl.DataFrame]:
    """
    Compute condition-by-condition correlations for each (population, reagent) pair.
    
    Args:
        pivot_tables: Dictionary mapping (population, reagent) tuples to pivot tables
                     where rows are donors and columns are conditions
        
    Returns:
        Dictionary mapping (population, reagent) tuples to correlation matrices
    """
    print("Computing condition-by-condition correlations...")
    
    results = {}
    for (pop, reag), pivot_df in pivot_tables.items():
        print(f"Pivot DataFrame for ({pop}, {reag}) columns: {pivot_df.columns}")
        # Select only meaningful numeric columns (condition columns and median)
        meaningful_cols = [col for col in pivot_df.columns if col not in ["population", "reagent"] and (pivot_df.schema[col] == pl.Float64 or pivot_df.schema[col] == pl.Int64)]
        print(f"Selected meaningful columns: {meaningful_cols}")
        data = pivot_df.select(meaningful_cols).to_numpy()
        
        # Compute correlation matrix
        corr_matrix = np.corrcoef(data.T)
        
        # Create DataFrame with meaningful column names
        meaningful_names = meaningful_cols
        corr_df = pl.DataFrame(
            {name: corr_matrix[i] for i, name in enumerate(meaningful_names)}
        ).with_columns(pl.Series(name="condition", values=meaningful_names))
        
        results[(pop, reag)] = corr_df
    
    print(f"Computed correlations for {len(results)} (population, reagent) pairs")
    return results


def analyze_correlation_thresholds(
    reagent_correlations: Dict[Tuple[str, str], pl.DataFrame],
    population_correlations: Dict[Tuple[str, str], pl.DataFrame],
    condition_correlations: Dict[Tuple[str, str], pl.DataFrame],
    thresholds: List[float] = [0.3, 0.5, 0.7, 0.9]
) -> pl.DataFrame:
    """
    Analyze correlation patterns across different thresholds.
    
    Args:
        reagent_correlations: Dictionary of reagent-by-reagent correlation matrices
        population_correlations: Dictionary of population-by-population correlation matrices
        condition_correlations: Dictionary of condition-by-condition correlation matrices
        thresholds: List of correlation thresholds to analyze
        
    Returns:
        DataFrame containing statistics about correlation patterns at each threshold
    """
    print("Analyzing correlation patterns...")
    
    # Initialize results
    results = []
    
    # Analyze reagent correlations
    for (pop, cond), corr_df in reagent_correlations.items():
        for threshold in thresholds:
            # Count strong correlations
            numeric_cols = corr_df.select(pl.exclude("reagent")).columns
            strong_corrs = corr_df.select(pl.col(numeric_cols).abs() >= threshold).sum().sum()
            total_corrs = len(corr_df) * (len(corr_df) - 1)  # Exclude self-correlations
            
            results.append({
                "type": "reagent",
                "population": pop,
                "condition": cond,
                "threshold": threshold,
                "strong_correlations": strong_corrs,
                "total_correlations": total_corrs,
                "percentage": (strong_corrs / total_corrs) * 100 if total_corrs > 0 else 0
            })
    
    # Analyze population correlations
    for (reag, cond), corr_df in population_correlations.items():
        for threshold in thresholds:
            numeric_cols = corr_df.select(pl.exclude("population")).columns
            strong_corrs = corr_df.select(pl.col(numeric_cols).abs() >= threshold).sum().sum()
            total_corrs = len(corr_df) * (len(corr_df) - 1)
            
            results.append({
                "type": "population",
                "reagent": reag,
                "condition": cond,
                "threshold": threshold,
                "strong_correlations": strong_corrs,
                "total_correlations": total_corrs,
                "percentage": (strong_corrs / total_corrs) * 100 if total_corrs > 0 else 0
            })
    
    # Analyze condition correlations
    for (pop, reag), corr_df in condition_correlations.items():
        for threshold in thresholds:
            numeric_cols = corr_df.select(pl.exclude("condition")).columns
            strong_corrs = corr_df.select(pl.col(numeric_cols).abs() >= threshold).sum().sum()
            total_corrs = len(corr_df) * (len(corr_df) - 1)
            
            results.append({
                "type": "condition",
                "population": pop,
                "reagent": reag,
                "threshold": threshold,
                "strong_correlations": strong_corrs,
                "total_correlations": total_corrs,
                "percentage": (strong_corrs / total_corrs) * 100 if total_corrs > 0 else 0
            })
    
    # Convert to DataFrame
    results_df = pl.DataFrame(results)
    print("Completed correlation pattern analysis")
    return results_df


def save_correlation_matrices(
    reagent_correlations: Dict[Tuple[str, str], pl.DataFrame],
    population_correlations: Dict[Tuple[str, str], pl.DataFrame],
    condition_correlations: Dict[Tuple[str, str], pl.DataFrame],
    output_dir: Path
) -> None:
    """
    Save correlation matrices to CSV files.
    
    Args:
        reagent_correlations: Dictionary of reagent-by-reagent correlation matrices
        population_correlations: Dictionary of population-by-population correlation matrices
        condition_correlations: Dictionary of condition-by-condition correlation matrices
        output_dir: Directory to save the matrices
    """
    print("Saving correlation matrices...")
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save reagent correlations
    for (pop, cond), corr_df in reagent_correlations.items():
        filename = f"reagent_correlations_{pop}_{cond}.csv"
        corr_df.write_csv(output_dir / filename)
    
    # Save population correlations
    for (reag, cond), corr_df in population_correlations.items():
        filename = f"population_correlations_{reag}_{cond}.csv"
        corr_df.write_csv(output_dir / filename)
    
    # Save condition correlations
    for (pop, reag), corr_df in condition_correlations.items():
        filename = f"condition_correlations_{pop}_{reag}.csv"
        corr_df.write_csv(output_dir / filename)
    
    print("Saved all correlation matrices")


def plot_correlation_analysis(threshold_stats: pl.DataFrame, output_dir: str) -> None:
    """
    Create visualizations for correlation analysis.
    
    This function generates two plots:
    1. Number of correlations by threshold
    2. Percentage of total correlations by threshold
    
    The plots are saved as a single PNG file in the specified output directory.
    
    Args:
        threshold_stats: DataFrame with correlation threshold statistics
        output_dir: Directory to save the plots
    """
    # Convert to pandas for plotting
    df = threshold_stats.to_pandas()
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    
    # Plot number of correlations
    sns.lineplot(data=df,
                x="threshold",
                y="num_correlations",
                marker='o',
                color='blue',
                ax=ax1)
    ax1.set_title('Number of Correlations by Threshold')
    ax1.set_xlabel('Correlation Threshold')
    ax1.set_ylabel('Number of Correlations')
    
    # Plot percentage of total
    sns.lineplot(data=df,
                x="threshold",
                y="percent_of_total",
                marker='o',
                color='red',
                ax=ax2)
    ax2.set_title('Percentage of Total Correlations by Threshold')
    ax2.set_xlabel('Correlation Threshold')
    ax2.set_ylabel('Percentage of Total')
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'correlation_analysis.png'))
    plt.close()
