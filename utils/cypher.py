from os import getenv
from textwrap import dedent

from cryptography.fernet import Fernet
from cryptography.fernet import InvalidToken

from utils.exceptions import MissingEnvironmentVariable
from utils.exceptions import EnvironmentVariableIsIncorrect


class Cypher:
    """Provides methods to encrypt and decrypted data.

    Attributes
    ----------
        cypher_pass: Fernet key used to encrypt and decrypt messages.

    Methods
    -------
        encrypt(): Encrypts a message using a Fernet key.
        decrypt(): Decrypts a encrypted message using a Fernet key.

    Example usage
    -------------
    >>> cypher = Cypher()
    """
    def __init__(self):
        self.cypher_pass = getenv('CYPHER_PASS')

    def encrypt(self, message: str) -> str:
        """Encrypts a message using a Fernet key.

        Arguments
        ---------
            message `str`: Message to encrypt.

        Returns
        -------
            `str`: Returns an encrypted message.

        Example usage
        -------------
        >>> encrypt(message='data')
        >>> 'gAAAAABj8ZD1hKxtOdN3NIdYWD05B4Heo92'
        """
        if not self.cypher_pass:
            error_message = 'Missing environment variable $CYPHER_PASS.'
            raise MissingEnvironmentVariable(error_message)

        try:
            f = Fernet(self.cypher_pass)
        except ValueError:
            error_message = dedent(f"""\n
                Environment variable $CYPHER_PASS has an incorrect format.
                It must be 32 base64-encoded bytes.

                Example: LUKcjooz97LmRO3QfAWyNG0HoVzIuOTykSij73OQpLg=

                Check: https://cryptography.io/en/latest/fernet/#cryptography.fernet.Fernet.generate_key
            """)
            raise ValueError(error_message)

        try:
            encoded_message = message.encode()
        except AttributeError:
            error_message = dedent(f"""\n
                Received an invalid message to encrypt:

                >>> print(message)
                {message}
                >>> type(message)
                (type: {type(message)})

                Type should be <class 'str'>
            """)
            raise ValueError(error_message)

        encrypted_message = f.encrypt(encoded_message)

        return encrypted_message.decode()


    def decrypt(self, encrypted_message: str) -> str:
        """Decrypts a encrypted message using a Fernet key.

        Arguments
        ---------
            encrypted_message `str`: Message to decrypt.

        Returns
        -------
            `str`: Returns an decrypted message.

        Example usage
        -------------
        >>> decrypt(encrypted_message='gAAAAABj8ZD1hKxtOdN3NIdYWD05B4Heo92')
        >>> 'data'
        """
        if not self.cypher_pass:
            error_message = 'Missing environment variable $CYPHER_PASS.'
            raise MissingEnvironmentVariable(error_message)

        try:
            f = Fernet(self.cypher_pass)
        except ValueError:
            error_message = dedent("""\
                Environment variable $CYPHER_PASS has an incorrect format.
                It must be 32 `base64-encoded bytes`.

                Example: LUKcjooz97LmRO3QfAWyNG0HoVzIuOTykSij73OQpLg=

                Check: https://cryptography.io/en/latest/fernet/#cryptography.fernet.Fernet.generate_key
            """)
            raise ValueError(error_message)

        try:
            encrypted_message_decoded = encrypted_message.encode()
        except AttributeError:
            error_message = dedent(f"""\n
                Received an invalid message to decrypt:

                >>> print(message)
                {encrypted_message}
                >>> type(message)
                (type: {type(encrypted_message)})

                Type should be <class 'str'>
            """)
            raise ValueError(error_message)

        try:
            decrypted_message = f.decrypt(encrypted_message_decoded)
        except InvalidToken:
            error_message = 'Environment variable $CYPHER_PASS is incorrect.'
            raise EnvironmentVariableIsIncorrect(error_message)

        return decrypted_message.decode()
