__name__ = 'parquet-loader'
__description__ = 'Parquet file Load and Read from minio & S3'
__version__ = '0.0.4'
__url__ = 'https://github.com/Keunyoung-Jung/ParquetLoader'
__download_url__ = 'https://github.com/Keunyoung-Jung/ParquetLoader'
__install_requires__ = [
    "pandas<=1.2.0",
    "fastparquet==0.8.0",
    "s3fs"
],
__author__ = 'keyog',
__author_email__ = 'jgy206@gmail.com'
__license__ = 'Apache License 2.0'

from ParquetLoader.loader import *
from ParquetLoader.s3 import *