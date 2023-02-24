from typing import Dict
from typing import Generator

import gdown
from dynaconf.base import LazySettings
from getfilelistpy import getfilelist
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from utils.cypher import Cypher


def decrypt_secrets(
    secrets: LazySettings,
    prefix: str = 'SECRET_'
) -> Dict[str, str]:
    """Decripta as secrets lidas pelo Dynaconf (arquivo .secrets.toml)
    e retorna um novo objeto do tipo dicionário.

    Arguments
    ---------
        secrets `LazySettings`: Objeto com as secrets encriptadas

    Keyword Arguments
    -----------------
        prefix `str`: Lê apenas as secrets que contém este prefixo no nome (default: 'SECRET_')

    Returns
    -------
        `Dict[str, str]`: Retorna um objeto do tipo dicionário com as secrets
        decriptadas.

    Example usage
    -------------
    >>> from dynaconf import Dynaconf
    >>>
    >>> _secrets = Dynaconf(settings_files=['.secrets.toml'], environments=True, env='prod')
    >>> print(decrypt_secrets())
    >>> {'SECRET_AWS_ACCESS_KEY': '...', 'SECRET_AWS_ACCESS_SECRET': '...', [...]}
    """

    cypher = Cypher()

    return {
        f'{secret[0]}': cypher.decrypt(secret[1]) for secret in secrets if secret[0].startswith(prefix)
    }


def list_files_in_public_google_drive_folder(
    folder_id: str,
    api_key: str,
    fields: str = 'files(name,id)'
) -> Generator:
    """List files in public Google Drive folder.

    Arguments
    ---------
        folder_id `str`: Google Drive folder ID.
        api_key `str`: Google Drive API key.

    Keyword Arguments
    -----------------
        fields `str`: Fields to retrieve about the file. (default: 'files(name,id)')

    Yields
    ------
        `Generator`

    Example usage
    -------------
    >>> list_files_in_public_google_drive_folder(
        folder_id='1HnP8MBaLYhFl8Bato1PbaTJ-B35v0_Kp',
        api_key='MY_GOOGLE_DRIVE_API_KEY',
        fields='files(name,id)'
    )
    """

    resource = {
        'api_key': api_key,
        'id': folder_id,
        'fields': fields
    }

    google_drive = getfilelist.GetFileList(resource)

    for file in google_drive['fileList'][0]['files']:
        yield file


def download_public_google_drive_file(
    file_id: str,
    output_folder: str,
    quiet: bool = True
) -> None:
    """Download file from public Google Drive folder.

    Arguments
    ---------
        file_id `str`: File ID.
        output_folder `str`: Folder to stores the downloaded data.

    Keyword Arguments
    -----------------
        quiet `bool`: Show download progress or not. (default: True)

    Example usage
    -------------
    >>> download_public_google_drive_file(
        file_id='1aFrtK-8kfjYtup6SqVwKsPUvRv39cXrU',
        output_folder='datalake/bronze',
        quiet=True
    )
    """

    try:
        gdown.download(
            url=f'https://drive.google.com/uc?id={file_id}',
            output=output_folder,
            quiet=quiet
        )
    except Exception as e:
        print(e)


def get_cross_sell(cod_id_produto: int) -> DataFrame:
    """Gets the cross-sell products for an `cod_id_produto`.

    Arguments
    ---------
        cod_id_produto `int`

    Returns
    -------
        `DataFrame`

    Example usage
    -------------
    >>> get_cross_sell(cod_id_produto=65962).show()
    >>> # +--------------+----------------------------------+
        # |COD_ID_PRODUTO|COD_ID_PRODUTO_TARGET             |
        # +--------------+----------------------------------+
        # |65962         |[65960, 8394, 93032, 71757, 65961]|
        # +--------------+----------------------------------+
    """
    spark = (
        SparkSession
            .builder
            .master('local[*]')
            .appName('mercafacil_consumer_app_cross_sell')
            .getOrCreate()
    )

    cross_sell = (
        spark
            .read
            .format('parquet')
            .load('datalake/gold/cross_sell/*')
    )

    return cross_sell.filter(F.col('COD_ID_PRODUTO') == cod_id_produto)


def get_up_sell(cod_id_cliente: int) -> DataFrame:
    """Gets the up-sell products for an `cod_id_cliente`.

    Arguments
    ---------
        cod_id_cliente `int`

    Returns
    -------
        `DataFrame`

    Example usage
    -------------
    >>> get_up_sell(cod_id_cliente=1080).show()
    >>> # +--------------+--------------------------------------+
        # |COD_ID_CLIENTE|COD_ID_PRODUTO_TARGET                 |
        # +--------------+--------------------------------------+
        # |1080          |[68084, 98878, 104722, 104720, 105685]|
        # +--------------+--------------------------------------+
    """
    spark = (
        SparkSession
            .builder
            .master('local[*]')
            .appName('mercafacil_consumer_app_up_sell')
            .getOrCreate()
    )

    up_sell = (
        spark
            .read
            .format('parquet')
            .load('datalake/gold/up_sell/*')
    )

    return up_sell.filter(F.col('COD_ID_CLIENTE') == cod_id_cliente)
