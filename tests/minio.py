from ParquetLoader import S3Loader

def s3_data_loading():
    sl = S3Loader(
        s3_endpoint_url='http://localhost:9000',
        s3_access_key='minio',
        s3_secret_key='minio123',
        bucket='test',
        folder="test-data")
    print(sl.schema)
    for df in sl :
        print(df.head())
        
if __name__ == '__main__':
    s3_data_loading()