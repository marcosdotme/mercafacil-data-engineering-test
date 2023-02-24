from pathlib import Path
from utils.utils import list_files_in_public_google_drive_folder
from utils.utils import download_public_google_drive_file


def download_data_from_google_drive_folder(
    api_key: str,
    folder_id: str,
    output_folder: str,
    fields: str = 'files(name,id)',
    quiet: bool = True
) -> None:
    """Download data from Google Drive folder to `output_folder`.

    Arguments
    ---------
        api_key `str`: Google Drive API key.
        folder_id `str`: Google Drive folder ID.
        output_folder `str`: Folder to stores the downloaded data.

    Keyword Arguments
    -----------------
        fields `str`: Fields to retrieve about the file. (default: 'files(name,id)')
        quiet `bool`: Show download progress or not. (default: True)

    Example usage
    -------------
    >>> download_data_from_google_drive_folder(
        api_key='MY_GOOGLE_DRIVE_API_KEY',
        folder_id='1HnP8MBaLYhFl8Bato1PbaTJ-B35v0_Kp',
        output_folder='datalake/bronze'
    )
    """

    files = list_files_in_public_google_drive_folder(
        folder_id=folder_id,
        api_key=api_key,
        fields=fields
    )

    for file in files:
        download_public_google_drive_file(
            file_id=file['id'],
            output_folder=f"{Path(output_folder)}/{file['name']}",
            quiet=quiet
        )
