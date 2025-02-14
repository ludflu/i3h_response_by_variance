from response_by_variance.etl import response_and_variance_transform
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

    med = output_frame.drop("variance")

    medpivot = med.pivot(
        on="population",
        index=["reagent", "Condition"],
        values="median",
        # aggregate_function=pl.col("median").median(),
    )
    # print(medpivot)

    # write the output to a csv file
    medpivot.write_csv(f"{output_filepath}/output.csv")


if __name__ == "__main__":
    main()
