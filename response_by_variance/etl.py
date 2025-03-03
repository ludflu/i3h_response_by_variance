import polars as pl
import numpy as np
from response_by_variance.optimize import find_best_combos


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


# TODO this needs to be validated
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
        df.group_by(correlation_columns)
        .agg(pl.col(value_column).alias("values"))
        .with_columns(
            pl.concat_str(correlation_columns, separator=",").alias("group_key"),
            pl.col("values").list.len().alias("values_size"),
        )
        .drop(correlation_columns)
        .sort("values_size", descending=False)
    )

    # to build the correlation matrix, we need the vectors to be the same length
    # we can either truncate the vectors to the shortest length, or impute missing values
    # for now, we will truncate

    values = [arr.to_list() for arr in responses.get_column("values")]
    min_len = min(len(v) for v in values)
    truncated = [v[:min_len] for v in values]

    keys = responses.get_column("group_key")
    response_dict = dict(zip(keys, truncated))
    corr = pl.DataFrame(response_dict).corr()
    return corr


def preprocess(
    input_frame: pl.DataFrame,
    initial_filters: dict[str, str],
    basal_filters: dict[str, str],
    normalization_join: list[str],
    keep_columns: list[str],
    aggregation_columns: list[str],
    std_dev_count: int,
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
    std_dev_count: int,
    value_column: str = "value",
):
    preprocessed = preprocess(
        input_frame,
        initial_filters,
        basal_filters,
        normalization_join,
        keep_columns,
        aggregation_columns,
        std_dev_count,
        value_column,
    )
    aggregated = group_by_and_agg(preprocessed, aggregation_columns).select(
        keep_columns
    )

    cdf = correlation_transform(
        preprocessed,
        [
            "population",
            "reagent",
            "Condition",
        ],
        "normalized_value",
    )

    cdf.write_csv("correlation_matrix.csv")

    # TODO: prove that this optimization is actually working by writing a test with some generated sample data
    # best_combos = find_best_combos(cdf, aggregated)

    # TODO cross celltype summary score has not been validated
    # summary = summary_score(aggregated)
    # aggregated = aggregated.join(summary, on=["reagent", "Condition"], how="left")

    # aggregated = (
    #     aggregated.with_columns(
    #         (
    #             (
    #                 pl.col("median")
    #                 + pl.col("variance")
    #                 + pl.col("cross_celltype_summary_score")
    #             )
    #             / 3.0
    #         ).alias("summary_score")
    #     )
    #     .drop(["cross_celltype_summary_score"])
    #     .sort(by=["summary_score", "median", "variance"], descending=True)
    # )

    return aggregated
