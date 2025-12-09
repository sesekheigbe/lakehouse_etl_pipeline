# etl_utils.py
# Utility functions for the Lakehouse ETL pipeline

from pyspark.sql import DataFrame

def filter_positive(df: DataFrame, column: str) -> DataFrame:
    """
    Filters out rows where the specified column has a non-positive value.
    Useful for quantity or price filtering in Silver transformations.

    Parameters:
        df (DataFrame): Input Spark DataFrame
        column (str): Column name to apply the filter on

    Returns:
        DataFrame: Filtered DataFrame
    """
    return df.filter(df[column] > 0)


def rename_columns(df: DataFrame, rename_map: dict) -> DataFrame:
    """
    Renames multiple columns in a DataFrame based on a mapping.

    Parameters:
        df (DataFrame): Input Spark DataFrame
        rename_map (dict): Dictionary mapping old names → new names

    Returns:
        DataFrame: DataFrame with renamed columns
    """
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    return df
