"""
Performance Metrics and Indices Module for Immune Health Response Analysis.

This module provides functions for calculating various performance metrics and indices
used in immune response analysis:

1. Variance Index:
   - Measures response variability across conditions
   - Normalizes variance by mean response
   - Accounts for measurement reliability

2. Coverage Index:
   - Measures cell type detection capability
   - Calculates coverage percentage and efficiency
   - Uses adaptive detection thresholds

3. Response Index:
   - Measures response magnitude and quality
   - Incorporates signal-to-noise ratio
   - Accounts for sample reliability

4. Reagent Impact:
   - Measures reagent effectiveness per cell type
   - Identifies optimal conditions
   - Combines response magnitude and quality

5. Channel Efficiency:
   - Combines multiple metrics for overall efficiency
   - Normalizes and weights different aspects
   - Provides comprehensive performance scoring

The module uses Polars for efficient data manipulation and statistical calculations.
"""

import polars as pl

def calculate_variance_index(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate variance index for each cell type across different stimuli conditions.
    
    This function computes a comprehensive variance metric that:
    1. Measures response variability across conditions
    2. Normalizes variance by mean response magnitude
    3. Accounts for measurement reliability based on condition count
    
    The final variance index combines:
    - Normalized variance (coefficient of variation)
    - Reliability factor based on condition count
    
    Args:
        df: DataFrame with normalized values, must contain:
            - population: Cell type identifier
            - reagent: Stimulus identifier
            - Condition: Experimental condition
            - normalized_value: Normalized response value
        
    Returns:
        DataFrame with variance metrics for each cell type-reagent pair:
        - population: Cell type identifier
        - reagent: Stimulus identifier
        - variance_index: Combined variance score
        - variance_score: Raw variance
        - mean_response: Mean response magnitude
        - condition_count: Number of conditions measured
    """
    print("Calculating variance index...")
    # Group by cell type and reagent, calculate variance across conditions
    variance_index = (
        df.group_by(["population", "reagent"])
        .agg(
            pl.col("normalized_value").var().alias("variance_score"),
            pl.col("normalized_value").mean().abs().alias("mean_response"),
            pl.count("normalized_value").alias("condition_count")
        )
        # Add safety minimum value for mean_response to avoid division by zero
        .with_columns(
            pl.when(pl.col("mean_response") < 0.001).then(0.001).otherwise(pl.col("mean_response")).alias("safe_mean_response"),
            pl.when(pl.col("condition_count") < 1).then(1).otherwise(pl.col("condition_count")).alias("safe_condition_count")
        )
        .with_columns(
            # Calculate coefficient of variation (normalized variance score)
            (pl.col("variance_score") / pl.col("safe_mean_response")).alias("normalized_variance"),
            # Reliability factor based on number of conditions
            (1 - (1 / pl.col("safe_condition_count"))).alias("reliability_factor")
        )
        .with_columns(
            # Final variance index combines normalized variance and reliability
            (pl.col("normalized_variance") * pl.col("reliability_factor")).alias("variance_index")
        )
        .select(["population", "reagent", "variance_index", "variance_score", "mean_response", "condition_count"])
        .sort("variance_index", descending=True)
    )
    print(f"Generated variance index for {len(variance_index)} cell type-reagent combinations")
    return variance_index


def calculate_coverage_index(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate coverage index showing how many cell types each reagent can detect.
    
    This function evaluates reagent performance by:
    1. Using an adaptive detection threshold based on median response
    2. Counting unique cell types detected above threshold
    3. Calculating coverage percentage and detection efficiency
    
    The coverage metrics help identify reagents that:
    - Detect the most cell types
    - Have efficient detection (fewer measurements needed)
    
    Args:
        df: DataFrame with normalized values, must contain:
            - population: Cell type identifier
            - reagent: Stimulus identifier
            - normalized_value: Normalized response value
        
    Returns:
        DataFrame with coverage metrics for each reagent:
        - reagent: Stimulus identifier
        - detected_cell_types: Number of cell types detected
        - total_measurements: Total number of measurements
        - total_cell_types: Total unique cell types in dataset
        - coverage_percentage: Percentage of cell types detected
        - detection_efficiency: Detections per measurement
    """
    print("Calculating coverage index...")
    # Define detection threshold - consider a signal "detected" if it's above this value
    # Using median absolute value of normalized response
    detection_threshold = df.select(pl.col("normalized_value").abs().median()).item()
    
    # Count detectable cell types per reagent
    coverage = (
        df.filter(pl.col("normalized_value").abs() > detection_threshold)
        .group_by("reagent")
        .agg(
            pl.col("population").n_unique().alias("detected_cell_types"),
            pl.count("population").alias("total_measurements")
        )
        .with_columns(
            # Calculate total unique cell types in the dataset
            pl.lit(df.select(pl.col("population").n_unique()).item()).alias("total_cell_types"),
        )
        .with_columns(
            # Coverage percentage
            (pl.col("detected_cell_types") / pl.col("total_cell_types") * 100).alias("coverage_percentage"),
            # Efficiency metric (detections per measurement)
            (pl.col("detected_cell_types") / pl.col("total_measurements")).alias("detection_efficiency")
        )
        .sort("coverage_percentage", descending=True)
    )
    print(f"Generated coverage index for {len(coverage)} reagents")
    return coverage


def calculate_response_index(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate response index measuring magnitude of cell type reactions compared to baseline.
    
    This function evaluates response quality by:
    1. Measuring response magnitude and variability
    2. Calculating signal-to-noise ratio
    3. Accounting for sample reliability
    
    The final response index combines:
    - Absolute response magnitude
    - Signal-to-noise ratio
    - Sample reliability factor
    
    Args:
        df: DataFrame with normalized values, must contain:
            - population: Cell type identifier
            - reagent: Stimulus identifier
            - Condition: Experimental condition
            - normalized_value: Normalized response value
        
    Returns:
        DataFrame with response metrics for each combination:
        - population: Cell type identifier
        - reagent: Stimulus identifier
        - Condition: Experimental condition
        - response_index: Combined response score
        - mean_response: Mean response value
        - absolute_mean_response: Mean absolute response
        - response_std: Response standard deviation
        - signal_to_noise: Signal-to-noise ratio
        - sample_reliability: Reliability factor
    """
    print("Calculating response index...")
    # Group by cell type, reagent, and condition
    response_index = (
        df.group_by(["population", "reagent", "Condition"])
        .agg(
            pl.col("normalized_value").mean().alias("mean_response"),
            pl.col("normalized_value").abs().mean().alias("absolute_mean_response"),
            pl.col("normalized_value").std().alias("response_std"),
            pl.count("normalized_value").alias("sample_count")
        )
        .filter(pl.col("Condition") != "Basal")  # Filter out basal conditions
        # Add safety minimum values
        .with_columns(
            pl.when(pl.col("response_std") < 0.001).then(0.001).otherwise(pl.col("response_std")).alias("safe_response_std"),
            pl.when(pl.col("sample_count") < 1).then(1).otherwise(pl.col("sample_count")).alias("safe_sample_count")
        )
        .with_columns(
            # Signal-to-noise ratio
            (pl.col("absolute_mean_response") / pl.col("safe_response_std")).alias("signal_to_noise"),
            # Consistency factor based on sample count
            (1 - (1 / pl.col("safe_sample_count"))).alias("sample_reliability")
        )
        .with_columns(
            # Final response index combines absolute response and signal quality
            (pl.col("absolute_mean_response") * pl.col("signal_to_noise") * pl.col("sample_reliability")).alias("response_index")
        )
        .sort("response_index", descending=True)
    )
    print(f"Generated response index for {len(response_index)} cell type-reagent-condition combinations")
    return response_index


def calculate_reagent_impact(response_index: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate reagent impact index showing how well each reagent detects changes in specific cell types.
    
    This function evaluates reagent effectiveness by:
    1. Identifying maximum response per cell type
    2. Calculating average signal quality
    3. Finding optimal conditions for each combination
    
    The impact score combines:
    - Maximum response index
    - Average signal-to-noise ratio
    - Response magnitude
    
    Args:
        response_index: DataFrame with response metrics from calculate_response_index()
        
    Returns:
        DataFrame with impact metrics for each reagent-cell type pair:
        - reagent: Stimulus identifier
        - population: Cell type identifier
        - max_response_index: Maximum response score
        - avg_signal_to_noise: Average signal quality
        - avg_response_magnitude: Average response size
        - best_condition: Optimal condition
        - reagent_impact_score: Combined impact score
    """
    print("Calculating reagent impact index...")
    
    # Get max response_index per group
    max_response = (
        response_index
        .group_by(["reagent", "population"])
        .agg(pl.col("response_index").max().alias("max_response_index"))
    )
    
    # Get mean signal_to_noise per group
    avg_signal = (
        response_index
        .group_by(["reagent", "population"])
        .agg(
            pl.col("signal_to_noise").mean().alias("avg_signal_to_noise"),
            pl.col("absolute_mean_response").mean().alias("avg_response_magnitude")
        )
    )
    
    # Instead of using rank, we'll use a simpler approach to get best conditions
    # First, join the max values back to the original dataframe
    response_with_max = response_index.join(
        max_response, 
        on=["reagent", "population"], 
        how="inner"
    )
    
    # Filter to get only the rows with max response values
    best_conditions = (
        response_with_max
        .filter(pl.col("response_index") == pl.col("max_response_index"))
        .select(["reagent", "population", "Condition"])
        .rename({"Condition": "best_condition"})
    )
    
    # Join all the data together
    reagent_impact = (
        max_response
        .join(avg_signal, on=["reagent", "population"], how="inner")
        .join(best_conditions, on=["reagent", "population"], how="inner")
        .with_columns(
            # Calculate final impact score
            (pl.col("max_response_index") * pl.col("avg_signal_to_noise")).alias("reagent_impact_score")
        )
        .sort("reagent_impact_score", descending=True)
    )
    
    print(f"Generated reagent impact index for {len(reagent_impact)} reagent-cell type pairs")
    return reagent_impact


def calculate_channel_efficiency(
    variance_index: pl.DataFrame, 
    coverage_index: pl.DataFrame, 
    response_index: pl.DataFrame
) -> pl.DataFrame:
    """
    Calculate channel efficiency index combining multiple performance metrics for each reagent.
    
    This function provides a comprehensive efficiency score by:
    1. Normalizing and combining multiple metrics:
       - Variance index
       - Coverage percentage
       - Response index
       - Signal-to-noise ratio
    2. Using adaptive weighting based on metric distributions
    3. Accounting for missing or low values
    
    The efficiency score helps identify reagents that:
    - Have consistent performance across metrics
    - Show balanced capabilities
    - Are reliable and efficient
    
    Args:
        variance_index: DataFrame from calculate_variance_index()
        coverage_index: DataFrame from calculate_coverage_index()
        response_index: DataFrame from calculate_response_index()
        
    Returns:
        DataFrame with efficiency metrics for each reagent:
        - reagent: Stimulus identifier
        - coverage_percentage: Cell type coverage
        - detection_efficiency: Detection efficiency
        - norm_variance: Normalized variance score
        - norm_response: Normalized response score
        - norm_snr: Normalized signal-to-noise
        - efficiency_score: Combined efficiency score
    """
    print("Calculating channel efficiency index...")
    
    # Aggregate variance scores by reagent
    avg_variance = (
        variance_index
        .group_by("reagent")
        .agg(pl.col("variance_index").mean().alias("avg_variance_index"))
    )
    
    # Aggregate response scores by reagent
    avg_response = (
        response_index
        .group_by("reagent")
        .agg(
            pl.col("response_index").mean().alias("avg_response_index"),
            pl.col("signal_to_noise").mean().alias("avg_signal_to_noise")
        )
    )
    
    # Join all metrics and calculate efficiency score
    efficiency = (
        coverage_index
        .join(avg_variance, on="reagent", how="left")
        .join(avg_response, on="reagent", how="left")
        .with_columns([
            # Fill NaN values with 0
            pl.col("avg_variance_index").fill_null(0),
            pl.col("avg_response_index").fill_null(0),
            pl.col("avg_signal_to_noise").fill_null(0)
        ])
    )
    
    # Calculate max values for normalization
    max_variance = efficiency.select(pl.col("avg_variance_index").max()).item()
    max_response = efficiency.select(pl.col("avg_response_index").max()).item()
    max_snr = efficiency.select(pl.col("avg_signal_to_noise").max()).item()
    
    # Use safe max values
    safe_max_variance = max(max_variance, 0.001)
    safe_max_response = max(max_response, 0.001)
    safe_max_snr = max(max_snr, 0.001)
    
    # Calculate normalized metrics
    efficiency = (
        efficiency
        .with_columns(
            # Calculate normalized metrics (0-1 scale)
            (pl.col("avg_variance_index") / safe_max_variance).alias("norm_variance"),
            (pl.col("coverage_percentage") / 100).alias("norm_coverage"),
            (pl.col("avg_response_index") / safe_max_response).alias("norm_response"),
            (pl.col("avg_signal_to_noise") / safe_max_snr).alias("norm_snr")
        )
        .with_columns(
            # Final efficiency score (weighted combination of all metrics)
            (
                (pl.col("norm_coverage") * 0.3) + 
                (pl.col("norm_variance") * 0.3) + 
                (pl.col("norm_response") * 0.2) + 
                (pl.col("norm_snr") * 0.2)
            ).alias("channel_efficiency_score")
        )
        .sort("channel_efficiency_score", descending=True)
    )
    print(f"Generated channel efficiency index for {len(efficiency)} reagents")
    return efficiency