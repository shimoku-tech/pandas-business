from typing import List, Dict, Tuple

import datetime as dt
import pandas as pd
from pandas_flavor import register_dataframe_method


def create_year_month(df, date_col, new_col):
    """
    Creates a new column in the DataFrame with the year and month extracted from the specified date column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        date_col (str): The name of the date column.
        new_col (str): The name of the new column to be created.

    Returns:
        pd.DataFrame: The updated DataFrame with the new column added.
    """
    mask = df[date_col].notnull()

    df[new_col] = (
        df.loc[mask, date_col].dt.year.astype("str")
        + "-"
        + df.loc[mask, date_col].apply(lambda x: x.strftime("%m"))
    )
    return df


def create_year_week(df, date_col, new_col):
    """
    Creates a new column in the DataFrame with the year and week number extracted from the specified date column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        date_col (str): The name of the date column.
        new_col (str): The name of the new column to be created.

    Returns:
        pd.DataFrame: The updated DataFrame with the new column added.
    """
    mask = df[date_col].notnull()

    df[new_col] = (
        df.loc[mask, date_col].dt.year.astype("str")
        + "-"
        + df.loc[mask, date_col].apply(lambda x: x.strftime("%W"))
    )
    return df


def create_date(df, date_col, new_col):
    """
    Creates a new column in the DataFrame with only the date part extracted from the specified date column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        date_col (str): The name of the date column.
        new_col (str): The name of the new column to be created.

    Returns:
        pd.DataFrame: The updated DataFrame with the new column added.
    """
    df[new_col] = df[date_col].dt.date

    return df


def calculate_diff_between_dates(granularity: str, end_date, start_date):
    """
    Calculates the difference between two dates based on the specified granularity.

    Args:
        granularity (str): The granularity of the difference calculation. Valid values: 'daily', 'weekly', 'monthly'.
        end_date: The end date.
        start_date: The start date.

    Returns:
        int: The difference between the dates based on the specified granularity.
    """
    # python tricks etl_machine
    if granularity == "daily":
        diff = (end_date - start_date).days
    elif granularity == "weekly":
        diff = int((end_date - start_date).days / 7)
    elif granularity == "monthly":
        diff = (
            (end_date.year - start_date.year) * 12 + end_date.month - start_date.month
        )
    return diff


def cohort_base(
    df: pd.DataFrame,
    cohort_column: str,  # evento contador a 0: installation, contract, first_use, first_purchase
    cohort_row: str,  # fecha de transacción ya en formato cohort: purchase_date, purchase_month, purchase_week
    metric_target_col: str,  # billing
    metric_operation: str,  # 'sum'
) -> pd.DataFrame:
    """
    Performs cohort analysis on the DataFrame and aggregates the metric values based on the specified columns and operations.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cohort_column (str): The column representing the cohort event with a counter starting from 0.
        cohort_row (str): The column representing the cohort row (e.g., purchase date, purchase month, purchase week).
        metric_target_col (str): The column containing the metric values.
        metric_operation (str): The aggregation operation to apply to the metric values.

    Returns:
        pd.DataFrame: The DataFrame with aggregated cohort metrics.
    """

    df = (
        df.groupby(
            [
                cohort_row,
                cohort_column,
                "cohort_event",
                "row_granularity",
                "col_granularity",
                "metric_name",
            ]
        )
        .agg({metric_target_col: metric_operation})
        .reset_index()
        .rename(columns={metric_target_col: "metric_value"})
    )
    return df


def cohort_metrics(
    df: pd.DataFrame,
    cohort_row: str,
    cohort_column: str,  # evento contador a 0: installation, contract, first_use, first_purchase
    metrics: Dict[str, List[str]],  # {'billing': ['sum', 'mean']
):

    """
    Yields cohort analysis results for multiple metrics based on the specified DataFrame, cohort row, cohort column, and metric operations.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cohort_row (str): The column representing the cohort row.
        cohort_column (str): The column representing the cohort column.
        metrics (Dict[str, List[str]]): A dictionary mapping metric target columns to a list of metric operations.

    Yields:
        pd.DataFrame: The DataFrame with cohort metrics for each metric operation.
    """
    for metric_target_col, metric_operations in metrics.items():
        for metric_operation in metric_operations:
            df["metric_name"] = f"{metric_target_col}_{metric_operation}"
            yield cohort_base(
                df=df,
                cohort_column=cohort_column,
                cohort_row=cohort_row,
                metric_target_col=metric_target_col,
                metric_operation=metric_operation,
            )


def add_cohort_granularity_cols(
    df: pd.DataFrame,
    cohort_event_col: str,
    transaction_event_col: str,
    cohort_row_granularity: str,
    cohort_column_granularity: str,
    use_months: bool,
    create_cohort_cols: bool = True,
) -> pd.DataFrame:
    """
    Adds cohort granularity columns to the DataFrame based on the specified event columns, row and column granularities, and options.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cohort_event_col (str): The column representing the cohort event.
        transaction_event_col (str): The column representing the transaction event.
        cohort_row_granularity (str): The granularity of the cohort row. Valid values: 'monthly', 'weekly', 'daily'.
        cohort_column_granularity (str): The granularity of the cohort column. Valid values: 'monthly', 'weekly', 'daily'.
        use_months (bool): Indicates whether to use months for cohort column if cohort_column_granularity is 'monthly'.
        create_cohort_cols (bool, optional): Indicates whether to create cohort column. Defaults to True.

    Returns:
        pd.DataFrame: The updated DataFrame with cohort granularity columns added.
    """
    if create_cohort_cols:
        df["cohort_column"] = df.apply(
            lambda x: calculate_diff_between_dates(
                cohort_column_granularity, x[transaction_event_col], x[cohort_event_col]
            ),
            axis=1,
        )

    if use_months:
        df = create_year_month(df, transaction_event_col, "cohort_column")

    if cohort_row_granularity == "monthly":
        df = create_year_month(df, cohort_event_col, "cohort_row")

    elif cohort_row_granularity == "weekly":
        df = create_year_week(df, cohort_event_col, "cohort_row")

    elif cohort_row_granularity == "daily":
        df = create_date(df, cohort_event_col, "cohort_row")

    return df


def cohort_granularity_metrics(
    df: pd.DataFrame,
    cohort_event_cols: List[
        str
    ],  # e.g.: installation, contract, first_use, first_purchase (formato date/datetime)
    transaction_event_col: str,  # fecha de transacción: purchase_date, consultation_date (formato date/datetime)
    metrics: Dict[str, List[str]],  # {'billing': ['sum', 'mean']}
    row_col_granularities: List[Tuple[str, str]],
    use_months: bool,
):

    """
    Yields cohort granularity metrics for the specified DataFrame, cohort event columns, transaction event column, metrics, row and column granularities, and options.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cohort_event_cols (List[str]): The list of cohort event columns.
        transaction_event_col (str): The column representing the transaction event.
        metrics (Dict[str, List[str]]): A dictionary mapping metric target columns to a list of metric operations.
        row_col_granularities (List[Tuple[str, str]]): The list of tuples representing row and column granularities.
        use_months (bool): Indicates whether to use months for cohort column if cohort_column_granularity is 'monthly'.

    Yields:
        pd.DataFrame: The DataFrame with cohort granularity metrics.
    """
    for cohort_event_col in cohort_event_cols:
        for row_granularity, col_granularity in row_col_granularities:
            df.dropna(subset=[cohort_event_col, transaction_event_col], inplace=True)
            if len(df) == 0:
                continue
            df_temp = add_cohort_granularity_cols(
                df=df,
                cohort_event_col=cohort_event_col,
                transaction_event_col=transaction_event_col,
                cohort_row_granularity=row_granularity,
                cohort_column_granularity=col_granularity,
                use_months=use_months,
            )
            df_temp["cohort_event"] = cohort_event_col
            df_temp["row_granularity"] = row_granularity
            df_temp["col_granularity"] = col_granularity

            yield cohort_metrics(
                df=df_temp,
                cohort_row="cohort_row",
                cohort_column="cohort_column",
                metrics=metrics,
            )


@register_dataframe_method
def cohort(
        df: pd.DataFrame, cohort_event_cols: List,
        transaction_event_col: str,
        metrics: Dict[str, List[str]],
        row_col_granularities: List[Tuple],
        use_months: bool = False,
) -> pd.DataFrame:
    """
    Performs cohort analysis on the DataFrame with different row and column granularities and returns the aggregated results.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cohort_event_cols (List): The list of cohort event columns.
        transaction_event_col (str): The column representing the transaction event.
        metrics (Dict[str, List[str]]): A dictionary mapping metric target columns to a list of metric operations.
        row_col_granularities (List[Tuple]): The list of tuples representing row and column granularities.
        use_months (bool, optional): Indicates whether to use months for cohort column if cohort_column_granularity is 'monthly'.

    Returns:
        pd.DataFrame: The DataFrame with aggregated cohort metrics.
    """
    iterator = cohort_granularity_metrics(
        df=df,
        cohort_event_cols=cohort_event_cols,
        transaction_event_col=transaction_event_col,
        metrics=metrics,
        row_col_granularities=row_col_granularities,
        use_months=use_months,
    )
    df_ = pd.DataFrame()
    for i in iterator:
        for df_temp in i:
            df_ = pd.concat([df_, df_temp])

    return df_
