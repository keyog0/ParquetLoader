from fastparquet import ParquetFile
import pandas as pd
import random
import os
import glob
from time import time

class DataLoader:
    def __init__(self,
                 chunk_size : int =100000,
                 root_path : str = '.',
                 folder : str = 'data',
                 shuffle : bool = True,
                 random_seed : int = int((time() - int(time()))*100000),
                 columns : list = None,
                 depth : int = 0,
                 std_out: bool = True,
                 filters: list = None
                 ):
        self.chunk_size = chunk_size
        self.cache = None
        self.dataset = None
        self.counter = 0
        self.num = 0
        self.placeholder = []
        self.folder = folder
        self.generator_iterator = self.generator()
        self.select_columns = columns
        self.shuffle = shuffle
        self.std_out = std_out
        self.fp_obj = None
        self.depth = depth
        self.random_seed = random_seed
        self.filters = filters
        self.check_filter()
        
        if root_path == '.' :
            self.root_path = os.getcwd()
        else : 
            self.root_path = root_path
        
        try :
            self.initialize()
        except IndexError as e :
            print("IndexError :",e)
            print(f'"{self.root_path}" may be incorrect or it may be an empty folder.')
            exit()
        
    def initialize(self) :
        path = f'{self.root_path}/{self.folder}{"/*"*self.depth}/*.parquet'
        if self.std_out :
            print(f'Loading parquet list from \"{path}\"...',end='\r')
        
        all_paths = glob.glob(path)
        if self.std_out :
            print(f'Loading parquet list from \"{path}\"... complete')
        start_init_time = time()
        if self.std_out :
            print(f'{len(all_paths)}files Initialize ...',end='\r')
        if self.shuffle :
            random.Random(self.random_seed).shuffle(all_paths)
        self.fp_obj = ParquetFile(all_paths)
        if self.std_out :
            print(f'{len(all_paths)}files Initialize ... complete {round(time()-start_init_time,2)}sec')
        self.total_num = self.fp_obj.info['row_groups']
        self.shuffle_seed_list = random.Random(self.random_seed).sample(range(100000+self.total_num*10),self.total_num)
    def __next__(self):
        return next(self.generator_iterator)
    def __iter__(self) :
        return self.generator_iterator
    @property
    def schema(self) :
        return self.fp_obj.schema
    @property
    def columns(self) :
        return self.fp_obj.info['columns']
    @property
    def count(self) :
        return self.fp_obj.info['rows']
    def throw(self, type=None, value=None, traceback=None):
        raise StopIteration
    def close(self):
        try:
            self.throw(GeneratorExit)
        except (GeneratorExit, StopIteration):
            pass
        else:
            raise RuntimeError("generator ignored GeneratorExit")
    def generator(self):
        for df in self.fp_obj.iter_row_groups(filters=self.filters,columns=self.select_columns):
            if self.filters != None :
                df = self.filtering(df)
            if self.dataset is None :
                self.dataset = df
            else :
                if len(self.dataset) > self.chunk_size :
                    slicer = len(self.dataset) - self.chunk_size
                    if self.shuffle :
                        self.dataset = self.dataset.sample(frac=1,random_state=self.shuffle_seed_list[self.num]).reset_index(drop=True)
                    dataset_comp = self.dataset[:-slicer]
                    self.dataset = pd.concat([self.dataset[self.chunk_size:],df])
                    yield dataset_comp
                elif len(self.dataset) < self.chunk_size :
                    self.dataset = pd.concat([self.dataset,df])
                elif len(self.dataset) == self.chunk_size :
                    yield self.dataset
                    self.dataset = df
            self.cache = df
            self.counter += len(df)
            self.num += 1
            if self.std_out :
                print('data loading...',f'{self.num}/{self.total_num}',end='\r')
        if self.shuffle :
            self.dataset = self.dataset.sample(frac=1,random_state=self.shuffle_seed_list[self.num-1]).reset_index(drop=True)
        yield self.dataset
        self.dataset = None
        if self.std_out :
            print(self.counter,'data loaded complete!',end='\n')
            
    def filtering(self,df) :
        op = ''
        df_store = []
        for or_part in self.filters:
            tmp_df = df.copy()
            for and_part in or_part :
                col = and_part[0]
                op = and_part[1]
                val = and_part[2]
                if op == '==' or op == '=' :
                    tmp_df = tmp_df[tmp_df[col] == val]
                elif op == '>' :
                    tmp_df = tmp_df[tmp_df[col] > val]
                elif op == '>=' :
                    tmp_df = tmp_df[tmp_df[col] >= val]
                elif op == '<' :
                    tmp_df = tmp_df[tmp_df[col] < val]
                elif op == '<=' :
                    tmp_df = tmp_df[tmp_df[col] <= val]
                elif op == '!=' :
                    tmp_df = tmp_df[tmp_df[col] != val]
                elif op == 'in' :
                    tmp_df = tmp_df[tmp_df[col] in val]
                elif op == 'not in' :
                    tmp_df = tmp_df[tmp_df[col] not in val]
            df_store.append(tmp_df)
        concat_df = pd.concat(df_store)
        return concat_df.drop_duplicates()
    
    def check_filter(self) :
        try :
            op = ''
            if self.filters != None :
                for or_part in self.filters :
                    for and_part in or_part :
                        op = and_part[1]
                        if len(and_part) != 3 :
                            raise  IndexError
                        if op not in ['==','=','>','>=','<','<=','!=','in','not in'] :
                            raise ValueError
        except ValueError :
            print("ValueError :",f'"{op}" is wrong operator')
            exit()
        except IndexError :
            print("IndexError :",f'{self.filters} invalid filter')
            exit()