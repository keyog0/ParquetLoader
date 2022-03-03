from ParquetLoader import DataLoader

def local_data_loading():
    dl = DataLoader(shuffle=False,depth=0)
    print(dl.schema)
    for df in dl :
        print(df.head())

if __name__ == '__main__':
    local_data_loading()