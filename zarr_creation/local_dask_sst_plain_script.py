import xarray as xr
from tqdm import tqdm
import time
import s3fs
import getpass
import dask
from dask.distributed import Client, LocalCluster
import asyncio


CONSISTENT_VARS = {'time',
                    'lat',
                    'lon',
                    'dt_analysis',
                    'l2p_flags',
                    'quality_level',
                    'satellite_zenith_angle',
                    'sea_surface_temperature',
                    'sses_bias',
                    'sses_count',
                    'sses_standard_deviation',
                    'sst_dtime'}
                    
FILES_PER_CHUNK = 10
SOURCE_PATH = 's3://imos-data/IMOS/SRS/SST/ghrsst/L3S-1d/day/<year or */*.nc'
STORE_PATH = f's3://<your-bucket>'
CHUNK_SHAPE = 1000


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def read_dataset_inmemory(s3_path: str) -> xr.Dataset:
    s3 = s3fs.S3FileSystem(anon=False)
    data = xr.open_dataset(s3.open(s3_path), engine="h5netcdf")
    available_vars = set(data.variables)
    to_drop_vars = list(available_vars - CONSISTENT_VARS)
    data = data.drop_vars(to_drop_vars)
    return data


async def create_dask_cluster():
    dask_user = getpass.getuser()
    dask_address = 'localhost'
    dask_port = '0'
    with dask.config.set({'temporary_directory': f'/home/{dask_user}/dask/'}):
        # set up cluster and workers
        cluster = LocalCluster(n_workers=2, memory_limit='20GB', processes=True, 
                            threads_per_worker=5, dashboard_address=f':{dask_port}', ip=dask_address)
        client = Client(address=cluster.scheduler_address)
        print(f"Dask Dashboard: {client.dashboard_link}")
        return cluster, client


async def clean_up_cluster(client, cluster):
    client.close()
    cluster.close()


def get_essentials(source_path, store_path):
    s3 = s3fs.S3FileSystem(anon=False)
    glob_result = s3.glob(source_path)
    store = s3fs.S3Map(root=f'{store_path}', s3=s3, check=False)
    input_paths = []
    input_paths.extend(['s3://' + path for path in glob_result])
    all_chunked_paths = list(chunks(input_paths, FILES_PER_CHUNK))  # indicates how many files should go into a single chunk
    return store, all_chunked_paths


async def main():

    store, all_chunked_paths = get_essentials(SOURCE_PATH, STORE_PATH)
    cluster, client = await create_dask_cluster()

    start_time = time.time()
    print('************** Processing **************')
    for i in tqdm(range(len(all_chunked_paths))):
        overwrite = True if i == 0 else False
        futures = []
        for path in all_chunked_paths[i]:
            futures.append(client.submit(read_dataset_inmemory, path, retries=10))
        zarrs = client.gather(futures)
        ds = xr.concat(zarrs, dim='time', coords='minimal',compat='override',combine_attrs='override', fill_value='')
        chunked = ds.chunk(chunks=CHUNK_SHAPE)  # TODO: new chunking strategy?
        for var in chunked.data_vars:
            chunked[var].encoding = {}
        if overwrite:
            z = chunked.to_zarr(store, mode='w', consolidated=True, compute=False) # return delayed obj
        else:
            z = chunked.to_zarr(store, mode='a', append_dim='time', consolidated=True, compute=False)
        [future.release() for future in futures] # release worker's memory
        z.compute() 
    print('*********************************************')
    print("---------- Total: %.2f seconds ----------" % (time.time() - start_time))
    print('*********************************************')

    await clean_up_cluster(client, cluster)


if __name__ == '__main__':
    asyncio.run(main())
