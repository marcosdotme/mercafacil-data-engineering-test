from dynaconf import Dynaconf
from utils.utils import decrypt_secrets


_secrets = Dynaconf(
    settings_files=['.secrets.toml'],
    environments=True,
    env='prod'
)

secrets = decrypt_secrets(
    secrets=_secrets.items(),
    prefix='SECRET_'
)

settings = Dynaconf(
    settings_files=['settings.toml'],
    environments=True,
    env='prod'
)
