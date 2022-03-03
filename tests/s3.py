from ParquetLoader import S3Loader

def s3_data_loading():
    sl = S3Loader(
        bucket='mysterico-feature-store',
        folder="mongo-sentence-token-feature.parquet/uploadedYear=2022",
        depth=1)
    print(sl.schema)
    for df in sl :
        print(df.head())
        
if __name__ == '__main__':
    s3_data_loading()