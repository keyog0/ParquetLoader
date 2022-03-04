from fastparquet import ParquetFile
from ParquetLoader.loader import DataLoader
import s3fs
from time import time
import random

class S3Loader(DataLoader) :
    def __init__(self,
                 chunk_size : int =100000,
                 s3_endpoint_url: str = '',
                 s3_access_key: str ='',
                 s3_secret_key: str ='',
                 bucket : str = '.',
                 folder : str = 'data',
                 shuffle : bool = True,
                 random_seed : int = int((time() - int(time()))*100000),
                 columns : list = None,
                 depth : int = 0,
                 std_out: bool = True
                 ):
        self.client_dict = None
        if s3_endpoint_url != '' or \
            s3_access_key != '' or \
            s3_secret_key != '' :
            self.client_dict = self.create_s3_client_dict(s3_endpoint_url,
                                                     s3_access_key,
                                                     s3_secret_key)
        super().__init__(
            chunk_size=chunk_size,
            root_path=bucket,
            folder=folder,
            shuffle=shuffle,
            random_seed=random_seed,
            columns=columns,
            depth=depth,
            std_out=std_out
            )
        
    def initialize(self) :
        path = f'{self.root_path}/{self.folder}{"/*"*self.depth}/*.parquet'
        
        if self.client_dict != None :
            s3 = s3fs.S3FileSystem(client_kwargs=self.client_dict)
            fs = s3fs.core.S3FileSystem(client_kwargs=self.client_dict)
        else :
            s3 = s3fs.S3FileSystem()
            fs = s3fs.core.S3FileSystem()
        
        all_paths = fs.glob(path=path)
        if self.std_out :
            print(f'Loading parquet list from \"{path}\"... complete')
        start_init_time = time()
        if self.std_out :
            print(f'{len(all_paths)}files Initialize ...',end='\r')
        myopen = s3.open
        if self.shuffle :
            random.Random(self.random_seed).shuffle(all_paths)
        self.fp_obj = ParquetFile(
            all_paths,
            open_with=myopen,
            root=f'{self.root_path}/{self.folder}'
            )
        if self.std_out :
            print(f'{len(all_paths)}files Initialize ... complete {round(time()-start_init_time,2)}sec')
        self.total_num = self.fp_obj.info['row_groups']
        self.shuffle_seed_list = random.Random(self.random_seed).sample(range(100000+self.total_num*10),self.total_num)
        
    def create_s3_client_dict(self,
                              s3_endpoint_url,
                              s3_access_key,
                              s3_secret_key
                              ):
        import os
        client_dict = {}
        if s3_endpoint_url != '' :
            client_dict['endpoint_url'] = s3_endpoint_url
        if s3_access_key != '' :
            os.environ['AWS_ACCESS_KEY_ID'] = s3_access_key
        if s3_secret_key != '' :
            os.environ['AWS_SECRET_ACCESS_KEY'] = s3_secret_key
        
        return client_dict