import polars as pl
import numpy as np


def filter_by_group(
    df: pl.DataFrame, by_filter_columns: dict[str, str]
) -> pl.DataFrame:
    for column, value in by_filter_columns.items():
        df = df.filter(pl.col(column) == value)
    return df


def filter_by_group_negate(
    df: pl.DataFrame, by_filter_columns: dict[str, str]
) -> pl.DataFrame:
    for column, value in by_filter_columns.items():
        df = df.filter(pl.col(column) != value)
    return df


def filter_data(
    df: pl.DataFrame, initial_filters: dict[str, str], value_column="value"
) -> pl.DataFrame:
    return filter_by_group(df.drop_nans(value_column), initial_filters)


def remove_outliers(
    df: pl.DataFrame,
    by_grouping_columns: list[str],
    num_std_dev: int,
    value_column="value",
) -> pl.DataFrame:
    grouped_std_var = (
        df.group_by(by_grouping_columns)
        .agg(pl.col(value_column).std())
        .rename({value_column: "std"})
    )

    return df.join(grouped_std_var, how="inner", on=by_grouping_columns).filter(
        (pl.col(value_column) <= pl.col("std") * num_std_dev)
        & (pl.col(value_column) >= -pl.col("std") * num_std_dev)
    )


def normalize_by_basal(
    df: pl.DataFrame,
    basal_filters: dict[str, str],
    normalization_join: list[str],
    value_column="value",
) -> pl.DataFrame:
    base = (
        filter_by_group(df, basal_filters)
        .rename({value_column: "basal_value"})
        .drop("Condition")
    )

    non_base = filter_by_group_negate(df, basal_filters)

    return base.join(non_base, how="inner", on=normalization_join).with_columns(
        (pl.col(value_column) - pl.col("basal_value")).alias("normalized_value"),
    )


def group_by_and_agg(df: pl.DataFrame, group_by: list[str]) -> pl.DataFrame:
    med = (
        df.group_by(group_by)
        .agg(pl.col("normalized_value").median())
        .rename({"normalized_value": "median"})
    )
    var = (
        df.group_by(group_by)
        .agg(pl.col("normalized_value").var())
        .rename({"normalized_value": "variance"})
    )
    return med.join(var, how="inner", on=group_by)


def avg_across_cell_populations(
    df: pl.DataFrame, pivot_value_column: str, output_value_column: str
) -> pl.DataFrame:
    unique_populations = df.select(pl.col("population")).unique().to_series().to_list()
    medpivot = df.pivot(
        on="population",
        index=["reagent", "Condition"],
        values=pivot_value_column,
    ).with_columns(pl.mean_horizontal(unique_populations).alias(output_value_column))
    return medpivot.drop(unique_populations)


def summary_score(df: pl.DataFrame) -> pl.DataFrame:
    medpop = avg_across_cell_populations(df, "median", "average_cell_response")
    varpop = avg_across_cell_populations(df, "variance", "average_celltype_variance")

    response_weight = 1
    variance_weight = 0.5

    return (
        medpop.join(varpop, on=["reagent", "Condition"], how="inner")
        .with_columns(
            (
                (pl.col("average_cell_response") * response_weight)
                + (pl.col("average_celltype_variance") * variance_weight)
            ).alias("cross_celltype_summary_score")
        )
        .drop(["average_cell_response", "average_celltype_variance"])
    )


def correlation_transform(
    df: pl.DataFrame,
    correlation_columns: list[str],
    value_column: str,
) -> pl.DataFrame:
    responses = (
        df.drop_nans(value_column)
        .drop_nulls(correlation_columns)
        .select(correlation_columns + [value_column])
        .group_by(correlation_columns)
        .agg(pl.col(value_column).alias("values"))
    )
    # breakpoint()
    # Convert to numpy array and reshape for correlation calculation
    values_array = np.array([arr.to_numpy() for arr in responses.get_column("values")])
    print(values_array)

    # these arrays are not the same shape
    # from this, I infer that not every reagent has a response for every condition
    # for every cell type for every subject in the population

    # correlation_matrix = np.corrcoef(values_array)

    # # Create a DataFrame with the correlation matrix
    # correlation_df = pl.DataFrame(
    #     correlation_matrix,
    #     schema=[f"correlation_{i}" for i in range(correlation_matrix.shape[0])],
    # )

    # return correlation_df


def preprocess(
    input_frame: pl.DataFrame,
    initial_filters: dict[str, str],
    basal_filters: dict[str, str],
    normalization_join: list[str],
    keep_columns: list[str],
    aggregation_columns: list[str],
    std_dev_count: int = 3,
    value_column: str = "value",
):
    df = filter_data(input_frame, initial_filters, value_column)
    df = normalize_by_basal(df, basal_filters, normalization_join, value_column)
    df = remove_outliers(df, aggregation_columns, num_std_dev=std_dev_count)
    return df


def response_and_variance_transform(
    input_frame: pl.DataFrame,
    initial_filters: dict[str, str],
    basal_filters: dict[str, str],
    normalization_join: list[str],
    keep_columns: list[str],
    aggregation_columns: list[str],
    std_dev_count: int = 3,
    value_column: str = "value",
):
    df = preprocess(
        input_frame,
        initial_filters,
        basal_filters,
        normalization_join,
        keep_columns,
        aggregation_columns,
        std_dev_count,
        value_column,
    )
    df = group_by_and_agg(df, aggregation_columns).select(keep_columns)

    summary = summary_score(df)
    df = df.join(summary, on=["reagent", "Condition"], how="left")

    df = (
        df.with_columns(
            (
                (
                    pl.col("median")
                    + pl.col("variance")
                    + pl.col("cross_celltype_summary_score")
                )
                / 3.0
            ).alias("summary_score")
        )
        .drop(["cross_celltype_summary_score"])
        .sort(by=["summary_score", "median", "variance"], descending=True)
    )

    return df
