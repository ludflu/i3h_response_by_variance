from response_by_variance.etl import (
    response_and_variance_transform,
    avg_across_cell_populations,
)
import polars as pl
import os


def main():
    input_filepath = os.environ["INPUT_DIR"]
    output_filepath = os.environ["OUTPUT_DIR"]

    # we will use these columns to filter the initial data
    # in our case we are only interested in human data
    initial_filters = {
        "Species": "Human",
    }

    # we will use these columns to filter the basal data (no stimulus applied)
    basal_filters = {
        "Condition": "Basal",
    }

    # we will use these columns to join the basal and non-basal data
    normalization_join = [
        "population",
        "reagent",
        "Donor",
    ]

    # only these columns will be output
    keep_columns = [
        "population",
        "reagent",
        "Condition",
        "median",
        "variance",
    ]

    # we will use these columns to group the data before calculating the median and variance
    aggregation_columns = ["population", "reagent", "Condition"]

    input_frame = pl.read_csv(f"{input_filepath}/input.csv")

    output_frame = response_and_variance_transform(
        input_frame,
        initial_filters,
        basal_filters,
        normalization_join,
        keep_columns,
        aggregation_columns,
    )

    medpop = avg_across_cell_populations(
        output_frame, "median", "average_cell_response"
    )
    varpop = avg_across_cell_populations(
        output_frame, "variance", "average_cell_variance"
    )

    response_and_variance = medpop.join(
        varpop, on=["reagent", "Condition"], how="inner"
    ).sort(by=["average_cell_response", "average_cell_variance"], descending=True)

    response_and_variance.write_csv(f"{output_filepath}/output.csv")


if __name__ == "__main__":
    main()
