from pathlib import Path

from pyspark.sql import functions as F
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


def generate_cross_sell(
    spark_session: SparkSession,
    dataframe_vendas: DataFrame,
    cod_id_produto: int | str
):
    dataframe_vendas.createOrReplaceTempView('df_vendas')

    df = spark_session.sql(f"""
    SELECT
        v1.COD_ID_PRODUTO,
        v2.COD_ID_PRODUTO AS COD_ID_PRODUTO_TARGET,
        COUNT(*) AS NUM_QUANTIDADE_VENDAS
    FROM
        df_vendas v1
        INNER JOIN df_vendas v2 ON v2.COD_ID_VENDA_UNICO = v1.COD_ID_VENDA_UNICO
    WHERE
        v1.COD_ID_PRODUTO = '{cod_id_produto}'
        AND v2.COD_ID_PRODUTO != '{cod_id_produto}'
    GROUP BY
        v1.COD_ID_PRODUTO,
        v2.COD_ID_PRODUTO
    ORDER BY
        NUM_QUANTIDADE_VENDAS DESC
    LIMIT 5
    """)

    df = (
        df
        .groupby('COD_ID_PRODUTO')
        .agg(
            F.collect_set('COD_ID_PRODUTO_TARGET').alias('COD_ID_PRODUTO_TARGET')
        )
    )

    df.write.format('parquet').mode('overwrite').save(f'datalake/gold/cross_sell/')


def generate_up_sell(
    spark_session: SparkSession,
    dataframe_vendas: DataFrame,
    cod_id_cliente: int | str
):
    dataframe_vendas.createOrReplaceTempView('df_vendas')

    df = spark_session.sql(f"""
    WITH produtos_cliente_comprou AS (
        SELECT
            COD_ID_PRODUTO
        FROM
            df_vendas
        WHERE
            COD_ID_CLIENTE = '{cod_id_cliente}'
        GROUP BY
            COD_ID_PRODUTO
    )

    SELECT
        {cod_id_cliente} AS COD_ID_CLIENTE,
        v.COD_ID_PRODUTO,
        COUNT(*) AS NUM_QUANTIDADE_VENDAS    
    FROM
        df_vendas v
    WHERE
        NOT EXISTS (
            SELECT
                COD_ID_PRODUTO
            FROM
                produtos_cliente_comprou x
            WHERE
                x.COD_ID_PRODUTO = v.COD_ID_PRODUTO
        )
    GROUP BY
        v.COD_ID_PRODUTO
    ORDER BY
        NUM_QUANTIDADE_VENDAS DESC
    LIMIT 5
    """)

    df = (
        df
        .groupby('COD_ID_CLIENTE')
        .agg(
            F.collect_set('COD_ID_PRODUTO').alias('COD_ID_PRODUTO_TARGET')
        )
    )

    df.write.format('parquet').mode('overwrite').save(f'datalake/gold/up_sell/')
