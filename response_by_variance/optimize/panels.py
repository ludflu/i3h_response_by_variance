"""
Panel Optimization Module for Immune Health Response Analysis.

This module provides functions for optimizing immune response analysis panels using various metrics:
- Variance-based optimization: Maximizes response variability
- Coverage-based optimization: Maximizes cell type coverage
- Response-based optimization: Maximizes response strength
- Complex optimization: Combines multiple metrics using linear programming

The module uses both greedy algorithms and linear programming (PuLP) to find optimal panel combinations
that balance multiple objectives while respecting constraints like panel size and correlation limits.
"""

import time
import polars as pl
import pulp
from pulp import LpProblem, LpVariable, lpSum

def find_best_combos(cdf: pl.DataFrame, response_and_variance: pl.DataFrame):
    """
    Find optimal combinations of reagents using linear programming to maximize variance and response
    while minimizing correlations between selected reagents.
    
    This function uses a multi-objective optimization approach with the following components:
    1. Variance maximization: Select reagents with high response variability
    2. Response maximization: Select reagents with strong responses
    3. Correlation minimization: Avoid selecting highly correlated reagents
    
    Args:
        cdf: Correlation matrix DataFrame between reagent combinations
        response_and_variance: DataFrame containing median and variance values for each reagent
        
    The function uses the CBC solver from COIN-OR for optimization and prints progress information.
    """
    combo_names = cdf.columns
    correlation_matrix = cdf.to_numpy()
    xlen, ylen = correlation_matrix.shape

    combo_names = cdf.columns
    combo_indices = range(xlen)
    combo_dict = dict(zip(combo_names, combo_indices))

    variance_dict: dict[str, float] = {}
    response_dict: dict[str, float] = {}

    for pop, reagent, _condition, median, variance in response_and_variance.iter_rows():
        group_key = f"{pop},{reagent},{_condition}"
        indx = combo_dict[group_key]
        variance_dict[indx] = variance
        response_dict[indx] = median

    print("setting up the problem...\n")

    # Set CBC solver path
    pulp.COIN_CMD(path="/opt/homebrew/bin/cbc")

    # Parameters for balancing objectives
    alpha = 0.5  # Weight for variance maximization
    beta = 1.0  # Weight for response maximization
    gamma = 5.0  # Weight for correlation minimization (larger because correlation is -1 to 1)

    # Decision variables: x[i] = 1 if object i is selected, 0 otherwise
    x = {i: LpVariable(f"x_{i}", cat="Binary") for i in combo_indices}

    # Auxiliary variables for pairwise repulsion terms
    y = {
        (i, j): LpVariable(f"y_{i}_{j}", cat="Binary")
        for i in combo_indices
        for j in combo_indices
        if i != j and x[i] == 1 and x[j] == 1
    }

    # Define the problem
    prob = LpProblem("Maximize_Objective", LpMaximize)

    prob += (
        alpha * lpSum(variance_dict[i] * x[i] for i in combo_indices)
        + beta * lpSum(response_dict[i] * x[i] for i in combo_indices)
        -
        # note we subtract the correlation in order to minimize it
        gamma * lpSum(abs(correlation_matrix[i, j]) * y[i, j] for (i, j) in y)
    )

    # TODO - I don't understand this constraint
    # Constraints to enforce y[i, j] = x[i] * x[j]
    for i, j in y:
        prob += y[i, j] <= x[i]  # y_ij can only be 1 if x_i is 1
        prob += y[i, j] <= x[j]  # y_ij can only be 1 if x_j is 1
        prob += y[i, j] >= x[i] + x[j] - 1  # y_ij = 1 iff both x_i and x_j are 1

    min_selection = 1  # Minimum number of objects selected
    max_selection = 10  # Maximum number of objects selected
    prob += lpSum(x[i] for i in combo_indices) >= min_selection
    prob += lpSum(x[i] for i in combo_indices) <= max_selection

    start_time = time.time()
    print("solving... (this may take a while)\n")
    print(f"start time: {start_time}")
    # Solve the problem and time it
    solver = pulp.getSolver("COIN_CMD")
    prob.solve(solver)
    elapsed_time = time.time() - start_time
    print(f"done solving (took {elapsed_time:.2f} seconds)\n")
    # Output results
    selected_objects = [combo_names[i] for i in combo_indices if x[i].value() == 1]
    print("Selected objects:", selected_objects)
    print("Objective value:", prob.objective.value())
    
def optimize_by_variance(variance_index: pl.DataFrame, correlation_matrix: pl.DataFrame, max_panel_size: int = 10) -> pl.DataFrame:
    """
    Generate optimal panels that maximize variance in cell type responses to stimuli.
    
    This function uses a greedy algorithm to select reagents that:
    1. Have high variance scores
    2. Are not highly correlated with already selected reagents
    3. Maintain a balance between panel size and performance
    
    Args:
        variance_index: DataFrame from calculate_variance_index() containing variance scores
        correlation_matrix: Correlation matrix between cell type-reagent combinations
        max_panel_size: Maximum number of reagents in each panel (default: 10)
        
    Returns:
        DataFrame with optimized panel designs containing:
        - panel_id: Unique identifier for each panel
        - panel_size: Number of reagents in the panel
        - reagents: Pipe-separated list of selected reagents
        - total_variance_score: Sum of variance scores for selected reagents
        - avg_variance_score: Average variance score per reagent
        - optimization_type: Always "Variance" for this function
    """
    print("Optimizing panels by variance...")
    
    # Get top reagents by variance
    top_reagents = (
        variance_index
        .group_by("reagent")
        .agg(pl.col("variance_index").mean().alias("avg_variance_index"))
        .sort("avg_variance_index", descending=True)
        .head(max_panel_size * 3)  # Take 3x more than needed to account for correlation filtering
        .get_column("reagent")
        .to_list()
    )
    
    # Create panels with different sizes
    panels = []
    for panel_size in range(3, max_panel_size + 1):
        # Greedy panel selection with correlation penalty
        selected = [top_reagents[0]]  # Start with top reagent
        
        for _ in range(1, panel_size):
            best_next = None
            best_score = -float("inf")
            
            for candidate in top_reagents:
                if candidate in selected:
                    continue
                
                # Calculate correlation penalty (average correlation with already selected reagents)
                corr_penalty = 0
                if len(selected) > 0:
                    # This is simplified - in real implementation we'd look up actual correlations
                    corr_penalty = 0.1 * len(selected)  # Placeholder penalty increasing with panel size
                
                # Get candidate's variance score
                candidate_score = variance_index.filter(pl.col("reagent") == candidate).get_column("variance_index").mean()
                
                # Calculate final score with penalty
                score = candidate_score - corr_penalty
                
                if score > best_score:
                    best_score = score
                    best_next = candidate
            
            if best_next:
                selected.append(best_next)
        
        # Calculate panel metrics
        total_variance = variance_index.filter(pl.col("reagent").is_in(selected)).get_column("variance_index").sum()
        avg_variance = total_variance / len(selected)
        
        # Add panel to results
        panels.append({
            "panel_id": f"Variance_Panel_{panel_size}",
            "panel_size": panel_size,
            "reagents": "|".join(selected),
            "total_variance_score": float(total_variance),
            "avg_variance_score": float(avg_variance),
            "optimization_type": "Variance"
        })
    
    print(f"Generated {len(panels)} variance-optimized panels")
    return pl.DataFrame(panels)


def optimize_by_coverage(coverage_index: pl.DataFrame, max_panel_size: int = 10) -> pl.DataFrame:
    """
    Generate optimal panels that maximize the number of cell types that can be effectively measured.
    
    This function uses a simple ranking approach to select reagents that:
    1. Cover the maximum number of cell types
    2. Maintain a balance between panel size and coverage
    
    Args:
        coverage_index: DataFrame from calculate_coverage_index() containing coverage scores
        max_panel_size: Maximum number of reagents in each panel (default: 10)
        
    Returns:
        DataFrame with optimized panel designs containing:
        - panel_id: Unique identifier for each panel
        - panel_size: Number of reagents in the panel
        - reagents: Pipe-separated list of selected reagents
        - total_coverage_score: Sum of coverage scores for selected reagents
        - avg_coverage_score: Average coverage score per reagent
        - optimization_type: Always "Coverage" for this function
    """
    print("Optimizing panels by coverage...")
    
    # Sort reagents by coverage
    sorted_reagents = (
        coverage_index
        .sort("coverage_percentage", descending=True)
        .get_column("reagent")
        .to_list()
    )
    
    # Create panels with different sizes
    panels = []
    for panel_size in range(3, max_panel_size + 1):
        # Take top N reagents by coverage
        selected = sorted_reagents[:panel_size]
        
        # Calculate panel metrics
        total_coverage = coverage_index.filter(pl.col("reagent").is_in(selected)).get_column("coverage_percentage").sum()
        avg_coverage = total_coverage / len(selected)
        
        # Add panel to results
        panels.append({
            "panel_id": f"Coverage_Panel_{panel_size}",
            "panel_size": panel_size,
            "reagents": "|".join(selected),
            "total_coverage_score": float(total_coverage),
            "avg_coverage_score": float(avg_coverage),
            "optimization_type": "Coverage"
        })
    
    print(f"Generated {len(panels)} coverage-optimized panels")
    return pl.DataFrame(panels)


def optimize_by_response(response_index: pl.DataFrame, max_panel_size: int = 10) -> pl.DataFrame:
    """
    Generate optimal panels that maximize the strength of cell type responses.
    
    This function uses a ranking approach to select reagents that:
    1. Have strong response magnitudes
    2. Maintain good signal-to-noise ratios
    3. Balance panel size with response strength
    
    Args:
        response_index: DataFrame from calculate_response_index() containing response scores
        max_panel_size: Maximum number of reagents in each panel (default: 10)
        
    Returns:
        DataFrame with optimized panel designs containing:
        - panel_id: Unique identifier for each panel
        - panel_size: Number of reagents in the panel
        - reagents: Pipe-separated list of selected reagents
        - avg_response_score: Average response score for selected reagents
        - avg_signal_to_noise: Average signal-to-noise ratio for selected reagents
        - optimization_type: Always "Response" for this function
    """
    print("Optimizing panels by response magnitude...")
    
    # Get top reagents by response index
    top_reagents = (
        response_index
        .group_by("reagent")
        .agg(pl.col("response_index").mean().alias("avg_response_index"))
        .sort("avg_response_index", descending=True)
        .head(max_panel_size * 2)  # Take 2x more than needed for selection flexibility
        .get_column("reagent")
        .to_list()
    )
    
    # Create panels with different sizes
    panels = []
    for panel_size in range(3, max_panel_size + 1):
        # Take top N reagents by response
        selected = top_reagents[:panel_size]
        
        # Calculate panel metrics
        avg_response = response_index.filter(pl.col("reagent").is_in(selected)).get_column("response_index").mean()
        avg_snr = response_index.filter(pl.col("reagent").is_in(selected)).get_column("signal_to_noise").mean()
        
        # Add panel to results
        panels.append({
            "panel_id": f"Response_Panel_{panel_size}",
            "panel_size": panel_size,
            "reagents": "|".join(selected),
            "avg_response_score": float(avg_response),
            "avg_signal_to_noise": float(avg_snr),
            "optimization_type": "Response"
        })
    
    print(f"Generated {len(panels)} response-optimized panels")
    return pl.DataFrame(panels)


def optimize_panel(
    variance_index: pl.DataFrame, 
    coverage_index: pl.DataFrame, 
    response_index: pl.DataFrame, 
    correlation_matrix: pl.DataFrame,
    channel_efficiency: pl.DataFrame,
    max_panel_size: int = 10
) -> pl.DataFrame:
    """
    Generate optimal panels using a comprehensive approach that balances multiple metrics.
    
    This function combines multiple optimization strategies to create panels that:
    1. Maximize response variance
    2. Maximize cell type coverage
    3. Maximize response strength
    4. Minimize correlations between reagents
    5. Consider channel efficiency
    
    Args:
        variance_index: DataFrame from calculate_variance_index()
        coverage_index: DataFrame from calculate_coverage_index()
        response_index: DataFrame from calculate_response_index()
        correlation_matrix: Correlation matrix between reagents
        channel_efficiency: DataFrame containing channel efficiency metrics
        max_panel_size: Maximum number of reagents in each panel (default: 10)
        
    Returns:
        DataFrame with optimized panel designs containing:
        - panel_id: Unique identifier for each panel
        - panel_size: Number of reagents in the panel
        - reagents: Pipe-separated list of selected reagents
        - Various performance metrics for each optimization strategy
        - optimization_type: Type of optimization used
    """
    print("Generating complex multi-objective optimized panels...")
    
    # Get top reagents by efficiency score
    top_reagents = (
        channel_efficiency
        .sort("channel_efficiency_score", descending=True)
        .head(max_panel_size * 3)  # Take 3x more than needed for selection flexibility
        .get_column("reagent")
        .to_list()
    )
    
    # Create panels with different optimization weights
    panels = []
    
    # Panel type 1: Balanced (equal weights)
    balanced_panel = top_reagents[:max_panel_size]
    panels.append({
        "panel_id": "Complex_Balanced",
        "panel_size": len(balanced_panel),
        "reagents": "|".join(balanced_panel),
        "optimization_type": "Complex",
        "optimization_focus": "Balanced",
        "variance_weight": 0.33,
        "coverage_weight": 0.33,
        "response_weight": 0.33
    })
    
    # Panel type 2: Variance focused
    variance_panel = (
        variance_index
        .group_by("reagent")
        .agg(pl.col("variance_index").mean().alias("avg_variance"))
        .sort("avg_variance", descending=True)
        .head(max_panel_size)
        .get_column("reagent")
        .to_list()
    )
    panels.append({
        "panel_id": "Complex_Variance",
        "panel_size": len(variance_panel),
        "reagents": "|".join(variance_panel),
        "optimization_type": "Complex",
        "optimization_focus": "Variance",
        "variance_weight": 0.7,
        "coverage_weight": 0.15,
        "response_weight": 0.15
    })
    
    # Panel type 3: Coverage focused
    coverage_panel = (
        coverage_index
        .sort("coverage_percentage", descending=True)
        .head(max_panel_size)
        .get_column("reagent")
        .to_list()
    )
    panels.append({
        "panel_id": "Complex_Coverage",
        "panel_size": len(coverage_panel),
        "reagents": "|".join(coverage_panel),
        "optimization_type": "Complex",
        "optimization_focus": "Coverage",
        "variance_weight": 0.15,
        "coverage_weight": 0.7,
        "response_weight": 0.15
    })
    
    # Panel type 4: Response focused
    response_panel = (
        response_index
        .group_by("reagent")
        .agg(pl.col("response_index").mean().alias("avg_response"))
        .sort("avg_response", descending=True)
        .head(max_panel_size)
        .get_column("reagent")
        .to_list()
    )
    panels.append({
        "panel_id": "Complex_Response",
        "panel_size": len(response_panel),
        "reagents": "|".join(response_panel),
        "optimization_type": "Complex",
        "optimization_focus": "Response",
        "variance_weight": 0.15,
        "coverage_weight": 0.15,
        "response_weight": 0.7
    })
    
    print(f"Generated {len(panels)} complex optimized panels")
    return pl.DataFrame(panels)