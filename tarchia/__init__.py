# isort: skip_file
from tarchia.__version__ import __author__
from tarchia.__version__ import __build__
from tarchia.__version__ import __version__

__all__ = ("__author__", "__build__", "__version__")

import datetime
import os

from pathlib import Path

# python-dotenv allows us to create an environment file to store secrets. If
# there is no .env it will fail gracefully.
try:
    import dotenv  # type:ignore
except ImportError:  # pragma: no cover
    dotenv = None  # type:ignore

_env_path = Path(".") / ".env"

# we do a separate check for debug mode here so we don't loaf the config
# module just yet
TARCHIA_DEBUG = os.environ.get("TARCHIA_DEBUG") is not None

#  deepcode ignore PythonSameEvalBinaryExpressiontrue: false +ve, values can be different
if _env_path.exists() and (dotenv is None):  # pragma: no cover
    # using a logger here will tie us in knots
    if TARCHIA_DEBUG:
        print(
            f"{datetime.datetime.now()} [LOADER] `.env` file exists but `python-dotenv` not installed."
        )
elif dotenv is not None:  # pragma: no cover variables from `.env`")
    dotenv.load_dotenv(dotenv_path=_env_path)
    if TARCHIA_DEBUG:
        print(f"{datetime.datetime.now()} [LOADER] Loading `.env` file.")

if TARCHIA_DEBUG:  # pragma: no cover
    from tarchia.debugging import DebuggingImportFinder
