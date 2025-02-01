from etl import response_and_variance_transform
import polars as pl


def main():
    initial_filters = {
        "Species": "Human",
    }

    basal_filters = {
        "Condition": "Basal",
    }

    normalization_join = [
        "population",
        "reagent",
        "Donor",
    ]

    keep_columns = [
        "population",
        "reagent",
        "Condition",
        "median",
        "variance",
    ]

    aggregation_columns = ["population", "reagent", "Condition"]

    input_frame = pl.read_csv("sup.csv")

    output_frame = response_and_variance_transform(
        input_frame,
        initial_filters,
        basal_filters,
        normalization_join,
        keep_columns,
        aggregation_columns,
    )
    output_frame.write_csv("data_normalized.csv")


if __name__ == "__main__":
    main()
