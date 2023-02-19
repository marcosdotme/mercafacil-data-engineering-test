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
    """[description]

    Arguments
    ---------
        api_key `str`: [description]
        folder_id `str`: [description]
        output_folder `str`: [description]

    Keyword Arguments
    -----------------
        fields `str`: [description] (default: 'files(name,id)')
        quiet `bool`: [description] (default: True)

    Example usage
    -------------
    >>> extract_data_from_google_drive_folder()
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
