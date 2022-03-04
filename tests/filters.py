from ParquetLoader import S3Loader

def s3_data_loading():
    sl = S3Loader(
        chunk_size=10000,
        s3_endpoint_url='http://localhost:9000',
        s3_access_key='minio',
        s3_secret_key='minio123',
        bucket='test',
        folder="data",
        shuffle=False)
    print(sl.schema)
    for df in sl :
        print(df.tail(10))
        break
    
def filter_data():
    sl = S3Loader(
        chunk_size=10000,
        s3_endpoint_url='http://localhost:9000',
        s3_access_key='minio',
        s3_secret_key='minio123',
        bucket='test',
        folder="data",
        filters=[
            [("tf",">",10),("tf","<",13)],
            [("tf","==",1)]
            ],
        shuffle=False)
    print(sl.schema)
    for df in sl :
        print(df.tail(10))
        break

def wrong_op():
    sl = S3Loader(
        chunk_size=10000,
        s3_endpoint_url='http://localhost:9000',
        s3_access_key='minio',
        s3_secret_key='minio123',
        bucket='test',
        folder="data",
        filters=[[("tf","%",10)]],
        shuffle=False)
    print(sl.schema)
    for df in sl :
        print(df.tail(10))
        break

def wrong_filter():
    sl = S3Loader(
        chunk_size=10000,
        s3_endpoint_url='http://localhost:9000',
        s3_access_key='minio',
        s3_secret_key='minio123',
        bucket='test',
        folder="data",
        filters=[
            [("tf",">")]],
        shuffle=False)
    print(sl.schema)
    for df in sl :
        print(df.tail(10))
        break
        
if __name__ == '__main__':
    s3_data_loading()
    filter_data()
    wrong_op()
    wrong_filter()