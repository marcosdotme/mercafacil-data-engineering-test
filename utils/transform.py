from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession


def transform_dataframe_vendas(
    spark_session: SparkSession,
    datalake_path: str,
    file_format: str = 'csv',
    delimiter: str = ';',
    header: bool = True
) -> DataFrame:

    dataframe = (
        spark_session
            .read
            .options(delimiter=delimiter, header=header)
            .format(file_format)
            .load(f'{Path(datalake_path)}/vendas*')
    )

    # Apply some transformations...

    return dataframe


def transform_dataframe_produtos(
    spark_session: SparkSession,
    datalake_path: str,
    file_format: str = 'csv',
    delimiter: str = ';',
    header: bool = True
) -> DataFrame:

    dataframe = (
        spark_session
            .read
            .options(delimiter=delimiter, header=header)
            .format(file_format)
            .load(f'{Path(datalake_path)}/produtos*')
    )

    # Apply some transformations...

    return dataframe


def transform_dataframe_categorias_produtos(
    spark_session: SparkSession,
    datalake_path: str,
    file_format: str = 'csv',
    delimiter: str = ';',
    header: bool = True
) -> DataFrame:

    dataframe = (
        spark_session
            .read
            .options(delimiter=delimiter, header=header)
            .format(file_format)
            .load(f'{Path(datalake_path)}/categorias_produtos*')
    )

    # Apply some transformations...

    return dataframe


def transform_dataframe_clientes(
    spark_session: SparkSession,
    datalake_path: str,
    file_format: str = 'csv',
    delimiter: str = ';',
    header: bool = True
) -> DataFrame:

    dataframe = (
        spark_session
            .read
            .options(delimiter=delimiter, header=header)
            .format(file_format)
            .load(f'{Path(datalake_path)}/clientes*')
    )

    # Apply some transformations...

    return dataframe
