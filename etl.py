import polars as pl


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


def filter_data(df: pl.DataFrame, initial_filters: dict[str, str]) -> pl.DataFrame:
    return filter_by_group(df.drop_nans("value"), initial_filters)


def remove_outliers(
    df: pl.DataFrame, by_grouping_columns: list[str], num_std_dev: int
) -> pl.DataFrame:
    grouped_std_var = (
        df.group_by(by_grouping_columns)
        .agg(pl.col("value").std())
        .rename({"value": "std"})
    )

    return df.join(grouped_std_var, how="inner", on=by_grouping_columns).filter(
        (pl.col("value") <= pl.col("std") * num_std_dev)
        & (pl.col("value") >= -pl.col("std") * num_std_dev)
    )


def normalize_by_basal(
    df: pl.DataFrame, basal_filters: dict[str, str], normalization_join: list[str]
) -> pl.DataFrame:
    base = (
        filter_by_group(df, basal_filters)
        .rename({"value": "basal_value"})
        .drop("Condition")
    )

    non_base = filter_by_group_negate(df, basal_filters)

    return base.join(non_base, how="inner", on=normalization_join).with_columns(
        (pl.col("value") - pl.col("basal_value")).alias("normalized_value"),
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


def response_and_variance_transform(
    input_frame: pl.DataFrame,
    initial_filters: dict[str, str],
    basal_filters: dict[str, str],
    normalization_join: list[str],
    keep_columns: list[str],
    aggregation_columns: list[str],
):
    df = filter_data(input_frame, initial_filters)
    df = normalize_by_basal(df, basal_filters, normalization_join)
    df = remove_outliers(df, aggregation_columns, num_std_dev=3)
    df = group_by_and_agg(df, aggregation_columns)
    return df.select(keep_columns)
