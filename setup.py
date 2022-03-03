from setuptools import setup, find_packages
import ParquetLoader

setup(
    name                = ParquetLoader.__name__,
    version             = ParquetLoader.__version__,
    description         = ParquetLoader.__description__,
    author              = ParquetLoader.__author__,
    author_email        = ParquetLoader.__author_email__,
    license             = ParquetLoader.__license__,
    url                 = ParquetLoader.__url__,
    download_url        = ParquetLoader.__download_url__,
    install_requires    = ParquetLoader.__install_requires__,
    packages            = find_packages(exclude = []),
    long_description=open('./README.md', 'r', encoding='utf-8').read(),
    long_description_content_type="text/markdown",
    keywords            = ['parquet','loader','parquetloader'],
    python_requires     = '>=3',
    package_data        = {},
    classifiers         = [
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)