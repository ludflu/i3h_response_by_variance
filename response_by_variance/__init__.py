"""
Immune Health Response Analysis Package.

This package provides a comprehensive suite of tools for analyzing immune response data,
with a focus on understanding and optimizing immune cell responses to various stimuli.

Core Functionality:
1. Data Processing and Normalization
   - Efficient data loading and cleaning
   - Basal condition normalization
   - Statistical outlier removal
   - Variance-based filtering

2. Correlation Analysis
   - Reagent-population correlation computation
   - Correlation threshold analysis
   - Feature correlation identification
   - Correlation visualization

3. Panel Optimization
   - Variance-based optimization
   - Coverage-based optimization
   - Response-based optimization
   - Multi-metric optimization
   - Channel efficiency calculation

4. Performance Metrics
   - Variance index calculation
   - Coverage index calculation
   - Response index calculation
   - Reagent impact assessment
   - Channel efficiency scoring

5. Statistical Analysis
   - Signal-to-noise ratio calculation
   - Reliability assessment
   - Performance scoring
   - Metric normalization

Package Structure:
- etl: Data processing, filtering, normalization, and correlation analysis
  - processing.py: Core data processing pipeline
  - filtering.py: Data filtering operations
  - normalization.py: Data normalization methods
  - correlation.py: Correlation analysis tools

- optimize: Panel optimization and metric calculations
  - indices.py: Performance metric calculations
  - panels.py: Panel optimization algorithms
  - pivots.py: Data pivot table generation

- utils: Utility functions
  - save_results: Results saving and formatting
  - Other utility functions for data handling

Technical Features:
- Efficient data processing using Polars
- Linear programming for optimization (PuLP)
- Comprehensive correlation analysis
- Multiple optimization strategies
- Flexible output generation
- Detailed performance metrics
- Statistical reliability measures

Usage:
The package is designed to be used as a pipeline, with each step building on the previous:
1. Load and process raw data
2. Normalize and filter data
3. Calculate performance metrics
4. Analyze correlations
5. Optimize panels
6. Generate results and visualizations
"""

from .etl import (
    analyze_correlation_thresholds,
    plot_correlation_analysis
)
from .optimize import (
    calculate_variance_index,
    calculate_coverage_index,
    calculate_response_index,
    calculate_reagent_impact,
    calculate_channel_efficiency,
    optimize_by_variance,
    optimize_by_coverage,
    optimize_by_response,
    optimize_panel
)
from .utils import save_results

# Define public API
__all__ = [
    # Correlation analysis functions
    'analyze_correlation_thresholds',
    'plot_correlation_analysis',
    # Panel optimization functions
    'calculate_variance_index',
    'calculate_coverage_index',
    'calculate_response_index',
    'calculate_reagent_impact',
    'calculate_channel_efficiency',
    'optimize_by_variance',
    'optimize_by_coverage',
    'optimize_by_response',
    'optimize_panel',
    # Utility functions
    'save_results'
]