import polars as pl
from response_by_variance.pipeline import response_and_variance_transform

# Read the input data
input_data = pl.read_csv("data_testing/input/input.csv")

# Define the parameters for the pipeline
initial_filters = {
    "Species": "human"  # Example filter
}

basal_filters = {
    "Condition": "basal"  # Example basal condition
}

normalization_join = ["population", "reagent"]  # Columns to join on for normalization
keep_columns = ["population", "reagent", "Condition", "median", "variance"]
aggregation_columns = ["population", "reagent", "Condition"]
std_dev_count = 4  # Number of standard deviations for outlier removal

# Run the pipeline
result = response_and_variance_transform(
    input_frame=input_data,
    initial_filters=initial_filters,
    basal_filters=basal_filters,
    normalization_join=normalization_join,
    keep_columns=keep_columns,
    aggregation_columns=aggregation_columns,
    std_dev_count=std_dev_count
)

# Print the results
print("\nProcessed Data Summary:")
print("----------------------")
print(f"Number of rows in result: {len(result)}")
print("\nFirst few rows of result:")
print(result.head()) 