# I3H Response and Variance ETL Documentation

This library provides functionality for processing and transforming experimental data, particularly focused on normalizing and analyzing reagent readouts from Cytometry by time of flight (CyTOF).

It assists in the design of immune assay panels by providing a structured way to find combinations of cell types, stimuli, and reagent readouts that are most informative because they show:
- Robust response
- Wide variance across a patient population
- Low correlation with other selected combinations

## Package Structure

The package is organized into the following modules:

### ETL Module (`response_by_variance/etl/`)
- `filtering.py`: Data filtering and cleaning operations
- `normalization.py`: Data normalization functions
- `processing.py`: Core data processing functions
- `correlation.py`: Correlation analysis utilities

### Optimization Module (`response_by_variance/optimize/`)
- `indices.py`: Index calculation and management
- `panels.py`: Panel optimization algorithms
- `pivots.py`: Data pivoting and transformation utilities

### Utilities Module (`response_by_variance/utils/`)
- `io.py`: Input/output operations and file handling

## Key Features

### Data Processing
- Filter and clean experimental data
- Remove outliers based on statistical measures
- Normalize values against baseline measurements
- Calculate group statistics and variance metrics

### Optimization
- Panel combination optimization
- Correlation analysis
- Response and variance scoring
- Population-level analysis

## Installation

### Using Docker
```bash
# Build the Docker image
make docker-build

# Pull the latest image
docker pull ludflu/i3h-response-and-variance
```

### Local Development
```bash
# Install dependencies
poetry install

# Run tests
make test
```

## Usage

The package can be used both as a library and through the command-line interface:

```python
from response_by_variance.etl import filtering, normalization
from response_by_variance.optimize import panels, indices

# Process your data
filtered_data = filtering.filter_data(df, initial_filters)
normalized_data = normalization.normalize_by_basal(filtered_data, basal_filters)

# Optimize panel combinations
optimized_panels = panels.optimize_panels(normalized_data)
```

## Testing

Run the test suite with:
```bash
make test
```

## Contributing

1. Create a new branch for your feature
2. Make your changes
3. Run tests to ensure everything works
4. Submit a pull request

## Team

This work came out of the Immune Atlas Hackathon Team at the The Immune Health Hackathon 2025. Sponsored by:

- The Colton Consortium
- The Institute for Immunology and Immune Health (I3H)
- Penn Institute for Biomedical Informatics

### Team Members
- Seljuq Haider
- Kelvin Koser
- Jen Shi
- Jim Snavely
- Kevin Wang
- Charles Zheng
