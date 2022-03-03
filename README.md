# ParquetLoader
<a href="https://github.com/Keunyoung-Jung/ParquetLoader/releases"><img alt="GitHub release" src="https://img.shields.io/github/release/Keunyoung-Jung/ParquetLoader.svg" /></a> 
<a href="https://github.com/Keunyoung-Jung/ParquetLoader/issues"><img alt="Issues" src="https://img.shields.io/github/issues/Keunyoung-Jung/ParquetLoader"/></a>    
Parquet file Load and Read from minio &amp; S3 or Local
This repository help read parquet file, When you train model or Analysis using bigdata.

## 1. Installation
### 1.1. Install from pip
```shell
pip install parquet-loader
```
### 1.2 Install from source codes
```shell
git clone https://github.com/Keunyoung-Jung/ParquetLoader
cd ParquetLoader
pip install -e .
```
## 2. Introduce
**ParquetLoader** can help to read large parquet files.    
**ParquetLoader** is built on base of pandas and fastparquet, which helps in situations where Spark clusters are not available.

It proceeds to load data into memory based on chuck size.    
Then return it as a pandas dataframe.    
## 3. Quick Start
### 3.1. Local Path
If your file is located `local`, you can load the data this way.
```python
from ParquetLoader import DataLoader

dl = DataLoader(
    folder='parquet_data'
    shuffle=False
    )
for df in dl :
    print(df.head())
```
### 3.2. S3 Path
If your file is located `S3` or `Minio`, you have to set 
environment variable.    
```shell
export AWS_ACCESS_KEY_ID=my-access-key
export AWS_SECRET_ACCESS_KEY=my-secret-key
export AWS_DEFAULT_REGION=ap-northeast-2
```
When you have finished setting, you can load data this way.   
```python
from ParquetLoader import S3Loader

sl = S3Loader(
        bucket='mysterico-feature-store',
        folder="mongo-sentence-token-feature",
        depth=2)

for df in sl :
    print(df.head())
```

## 4. Parameters
`ParquetLoader` can control reading data using parameters.
The only difference between `S3Loader` and `DataLoader` is the `root_path` parameter.
```python
dl = DataLoader(
    chunk_size : int =100000,
    root_path : str = '.', # S3Loader using "bucket"
    folder : str = 'data',
    shuffle : bool = True,
    random_seed : int = int((time() - int(time()))*100000),
    columns : list = None,
    depth : int = 0,
    std_out: bool = True
    )
```
* `chunk_size`
    * default : 100,000 row 
    * This parameter controls the number of rows loaded into memory when reading data.
* `root_path` or `bucket`
    * default : current path
    * This parameter is used to specify the project path or datastore path.
* `folder`
    * default : "data"
    * This parameter specifies the actual folder in which the parquet is clustered.
* `shuffle`
    * default : True
    * Whether to shuffle the data.
* `random_seed`
    * default : `int((time() - int(time()))*100000)`
    * You can fix the order of the shuffled data by giving it a random seed.
* `columns`
    * default : None
    * When reading data, you can select columns.
* `depth`
    * default : 0
    * It is used when the parquet in the folder is partitioned and there is depth.
* `std_out`
    * default : True
    * You can turn off output.

### 4.1. Select Columns
`columns` param is taken as a list.
```python
dl = DataLoader(
    folder='parquet_data',
    colums=['name','age','gender']
    )
```
### 4.2. Setting depth
Use if your parquet file is partitioned and depth exists.    
**Example**
```
📦 data    
 ┣ 📦 Year=2020     
    ┣ 📜 part-0000-example.snappy.parquet   
    ┗ 📜 part-0001-example.snappy.parquet  
 ┣ 📦 Year=2021     
    ┣ 📜 part-0002-example.snappy.parquet   
    ┗ 📜 part-0003-example.snappy.parquet  
```
The data path in this example has a `depth 1`.
```python
dl = DataLoader(
    folder='parquet_data',
    depth=1
    )
```

## 5. Get Metadata
`DataLoader` Object can get metadata your parquet
```python
print(data_loader.schema) # get data schema
print(data_loader.columns) # get data columns
print(data_loader.count) # get total count
print(data_loader.info) # get metadata infomation
```