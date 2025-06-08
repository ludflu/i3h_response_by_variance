"""
ETL Module for Immune Health Response Analysis.

This module provides functions for:
1. Data processing and filtering
2. Pivot table generation
3. Correlation analysis
"""

from .processing import (
    process_data,
    aggregate_data,
    filter_data,
    calculate_variance_index
)

from response_by_variance.optimize.pivots import (
    create_population_reagent_pivot,
    create_population_condition_pivot,
    create_reagent_condition_population_pivot,
    create_variance_pivot
)

from .correlation import (
    compute_reagent_correlations,
    compute_population_correlations,
    compute_condition_correlations,
    analyze_correlation_thresholds,
    save_correlation_matrices,
    plot_correlation_analysis
)

__all__ = [
    # Processing functions
    'process_data',
    'aggregate_data',
    'filter_data',
    'calculate_variance_index',
    
    # Pivot functions
    'create_population_reagent_pivot',
    'create_population_condition_pivot',
    'create_reagent_condition_population_pivot',
    'create_variance_pivot',
    
    # Correlation functions
    'compute_reagent_correlations',
    'compute_population_correlations',
    'compute_condition_correlations',
    'analyze_correlation_thresholds',
    'save_correlation_matrices',
    'plot_correlation_analysis'
]
