from typing import Dict
from typing import Generator

import gdown
from dynaconf.base import LazySettings
from getfilelistpy import getfilelist

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
    """[description]

    Arguments
    ---------
        folder_id `str`: [description]
        api_key `str`: [description]

    Keyword Arguments
    -----------------
        fields `str`: [description] (default: 'files(name,id)')

    Yields
    ------
        `Generator`: [description]

    Example usage
    -------------
    >>> list_files_in_public_google_drive_folder()
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
    """[description]

    Arguments
    ---------
        file_id `str`: [description]
        output_folder `str`: [description]

    Keyword Arguments
    -----------------
        quiet `bool`: [description] (default: True)

    Example usage
    -------------
    >>> download_public_google_drive_file()
    """

    try:
        gdown.download(
            url=f'https://drive.google.com/uc?id={file_id}',
            output=output_folder,
            quiet=quiet
        )
    except Exception as e:
        print(e)
