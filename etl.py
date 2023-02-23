from pyspark.sql import SparkSession

from dynaconf_config import secrets
from dynaconf_config import settings
from utils.extract import download_data_from_google_drive_folder
from utils.transform import generate_cross_sell
from utils.transform import generate_up_sell
from utils.transform import transform_dataframe_categorias_produtos
from utils.transform import transform_dataframe_clientes
from utils.transform import transform_dataframe_produtos
from utils.transform import transform_dataframe_vendas


spark = (
    SparkSession
        .builder
        .master('local[*]')
        .appName('mercafacil_etl')
        .getOrCreate()
)


# Bronze layer
download_data_from_google_drive_folder(
    api_key=secrets['SECRET_GOOGLE_DRIVE_API_KEY'],
    folder_id=settings['SETTING_GOOGLE_DRIVE_FOLDER_ID'],
    output_folder='datalake/bronze'
)


# Silver layer
df_vendas = transform_dataframe_vendas(
    spark_session=spark,
    datalake_path='datalake/bronze',
    file_format='csv',
    delimiter=';',
    header=True
)
df_vendas.write.format('parquet').mode('overwrite').save('datalake/silver/vendas/')

df_produtos = transform_dataframe_produtos(
    spark_session=spark,
    datalake_path='datalake/bronze',
    file_format='csv',
    delimiter=';',
    header=True
)
df_produtos.write.format('parquet').mode('overwrite').save('datalake/silver/produtos/')

df_categorias_produtos = transform_dataframe_categorias_produtos(
    spark_session=spark,
    datalake_path='datalake/bronze',
    file_format='csv',
    delimiter=';',
    header=True
)
df_categorias_produtos.write.format('parquet').mode('overwrite').save('datalake/silver/categorias_produtos/')

df_clientes = transform_dataframe_clientes(
    spark_session=spark,
    datalake_path='datalake/bronze',
    file_format='csv',
    delimiter=';',
    header=True
)
df_clientes.write.format('parquet').mode('overwrite').save('datalake/silver/clientes/')


# Gold layer
for cod_id_produto in [2069, 65962]:
    generate_cross_sell(spark_session=spark, dataframe_vendas=df_vendas, cod_id_produto=cod_id_produto)

for cod_id_cliente in [80, 1080]:
    generate_up_sell(spark_session=spark, dataframe_vendas=df_vendas, cod_id_cliente=cod_id_cliente)
