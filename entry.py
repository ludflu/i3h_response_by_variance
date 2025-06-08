"""
Main entry point for the Immune Health Response Analysis project.

This script processes input data through the following pipeline:
1. Basic data processing and cleaning
2. Data aggregation and filtering
3. Pivot table generation
4. Correlation analysis
5. Additional analyses (variance index, etc.)
"""

import os
import sys
import polars as pl
from pathlib import Path
from response_by_variance.etl import (
    process_data,
    aggregate_data,
    filter_data,
    calculate_variance_index,
    create_population_reagent_pivot,
    create_population_condition_pivot,
    create_reagent_condition_population_pivot,
    create_variance_pivot,
    compute_reagent_correlations,
    compute_population_correlations,
    compute_condition_correlations,
    analyze_correlation_thresholds,
    save_correlation_matrices,
    plot_correlation_analysis
)

def main():
    # Get input and output directories from environment variables
    input_dir = os.getenv("INPUT_DIR", "data/input")
    output_dir = os.getenv("OUTPUT_DIR", "data/output")
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print("Starting data processing pipeline...")
    
    # 1. Process data through the pipeline
    print("\n1. Processing data...")
    processed_data = process_data(input_dir)
    print(f"Basic processing complete. Shape: {processed_data.shape}")
    
    # 2. Aggregate and filter data
    print("\n2. Aggregating and filtering data...")
    aggregated_data = aggregate_data(processed_data)
    filtered_data = filter_data(aggregated_data)
    print(f"Aggregation and filtering complete. Shape: {filtered_data.shape}")
    
    # Save filtered data
    filtered_data.write_csv(os.path.join(output_dir, "output.csv"))
    print("Saved filtered data to output.csv")
    
    # 3. Generate pivot tables
    print("\n3. Generating pivot tables...")
    reagent_pivots = create_population_reagent_pivot(filtered_data)
    population_pivots = create_population_condition_pivot(filtered_data)
    condition_pivots = create_reagent_condition_population_pivot(filtered_data)
    variance_pivot = create_variance_pivot(filtered_data)
    
    # Save variance pivot
    variance_pivot.write_csv(os.path.join(output_dir, "variance_pivot.csv"))
    print("Saved variance pivot table")
    
    # 4. Run correlation analyses
    print("\n4. Running correlation analyses...")
    
    # Compute correlations
    reagent_correlations = compute_reagent_correlations(reagent_pivots)
    population_correlations = compute_population_correlations(population_pivots)
    condition_correlations = compute_condition_correlations(condition_pivots)
    
    # Analyze correlation patterns
    threshold_stats = analyze_correlation_thresholds(
        reagent_correlations,
        population_correlations,
        condition_correlations
    )
    
    # Save correlation matrices and analysis
    save_correlation_matrices(
        reagent_correlations,
        population_correlations,
        condition_correlations,
        Path(output_dir) / "correlations"
    )
    threshold_stats.write_csv(os.path.join(output_dir, "correlation_threshold_analysis.csv"))
    print("Saved correlation matrices and analysis")
    
    # 5. Run additional analyses
    print("\n5. Running additional analyses...")
    
    # Calculate variance index
    variance_index = calculate_variance_index(filtered_data)
    variance_index.write_csv(os.path.join(output_dir, "variance_index.csv"))
    print("Saved variance index")
    
    print("\nPipeline complete!")

if __name__ == "__main__":
    main()
