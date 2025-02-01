# I3H Response and Variance ETL Documentation

This library provides functionality for processing and transforming experimental data, particularly focused on normalizing and analyzing response measurements.
Its intended to assist in the design of immune assay panels, by providing a structured way to find combinations of cell types, stimuli and reagent
readouts that are most informative because they show a robust response and wide variance accross a patient population.

### Immune Atlas Hackathon Team

This work came out of the Immune Atlas Hackathon Team
at the The Immune Health Hackathon 2025. Sponsored by:

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

### Data Filtering

- `filter_by_group(df, by_filter_columns)`: Filter dataframe rows matching specified column values
- `filter_by_group_negate(df, by_filter_columns)`: Filter dataframe rows NOT matching specified column values
- `filter_data(df, initial_filters)`: Filter data and remove NaN values

### Data Processing

- `remove_outliers(df, by_grouping_columns, num_std_dev)`: Remove outliers based on standard deviation within groups
- `normalize_by_basal(df, basal_filters, normalization_join)`: Normalize values by subtracting baseline measurements
- `group_by_and_agg(df, group_by)`: Group data and calculate median and variance statistics

### Main Transform Pipeline

`response_and_variance_transform()` combines the above functions into a complete pipeline:

1. Filters initial data
2. Normalizes against baseline measurements
3. Removes outliers
4. Calculates group statistics

## Running Tests

You can run the pytest with the following command:

```
make test
```
