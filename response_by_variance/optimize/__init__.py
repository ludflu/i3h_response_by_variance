"""
Panel Optimization and Analysis Module for Immune Health Response Analysis.

This package provides functionality for optimizing immune response analysis panels
and calculating various performance metrics. It includes:

1. Index Calculations:
   - Variance index: Measures response variability
   - Coverage index: Measures population coverage
   - Response index: Measures response strength
   - Reagent impact: Measures reagent effectiveness
   - Channel efficiency: Measures overall efficiency

2. Panel Optimization:
   - Variance-based optimization
   - Coverage-based optimization
   - Response-based optimization
   - Complex multi-metric optimization

3. Pivot Table Generation:
   - Population-reagent relationships
   - Population-condition relationships
   - Complex reagent-condition-population analysis
   - Variance pattern analysis

The module uses linear programming (PuLP) for optimization and provides
various visualization tools for analysis results.
"""

from .indices import (
    calculate_variance_index,
    calculate_coverage_index,
    calculate_response_index,
    calculate_reagent_impact,
    calculate_channel_efficiency
)
from .panels import (
    find_best_combos,
    optimize_by_variance,
    optimize_by_coverage,
    optimize_by_response,
    optimize_panel
)
from .pivots import (
    create_population_reagent_pivot,
    create_population_condition_pivot,
    create_reagent_condition_population_pivot,
    create_variance_pivot
)

# Define public API
__all__ = [
    # Index calculation functions
    'calculate_variance_index',
    'calculate_coverage_index',
    'calculate_response_index',
    'calculate_reagent_impact',
    'calculate_channel_efficiency',
    
    # Panel optimization functions
    'find_best_combos',
    'optimize_by_variance',
    'optimize_by_coverage',
    'optimize_by_response',
    'optimize_panel',
    
    # Pivot table functions
    'create_population_reagent_pivot',
    'create_population_condition_pivot',
    'create_reagent_condition_population_pivot',
    'create_variance_pivot'
]