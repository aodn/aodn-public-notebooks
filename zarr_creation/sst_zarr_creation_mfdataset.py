import s3fs
import xarray as xr
from toolz import partition


CONSISTENT_VARS = {
    "time",
    "lat",
    "lon",
    "dt_analysis",
    "l2p_flags",
    "quality_level",
    "satellite_zenith_angle",
    "sea_surface_temperature",
    "sses_bias",
    "sses_count",
    "sses_standard_deviation",
    "sst_dtime",
}

CHUNKS = {"time": 10, "lon": 1500, "lat": 1500}


def get_files(source_path, s3):
    glob_result = s3.glob(source_path)

    return glob_result


def main(source_path, store_path):
    s3 = s3fs.S3FileSystem(anon=False)
    files = get_files(source_path, s3)

    first_file = True
    completed = 0
    batch = 20

    print(f"Found {len(files)} files to process in batches of {batch}")

    def drop_vars(dataset):
        available_vars = set(dataset.variables)
        to_drop_vars = list(available_vars - CONSISTENT_VARS)
        return dataset.drop_vars(to_drop_vars)

    for files_subset in partition(batch, files):
        paths = [s3.open(file) for file in files_subset]
        ds = xr.open_mfdataset(
            paths,
            engine="h5netcdf",
            preprocess=drop_vars,
        )

        ds = ds.chunk(CHUNKS)

        if first_file:
            ds.to_zarr(store_path, mode="w", consolidated=True)
            first_file = False
        else:
            ds.to_zarr(store_path, mode="a", append_dim="time", consolidated=True)

        completed += batch
        print(f"Finished {completed} out of {len(files)}")


if __name__ == "__main__":
    source_path = "s3://imos-data/IMOS/SRS/SST/ghrsst/L3S-1d/day/2022/*.nc"
    store_path = "s3://imos-data-pixeldrill/alexl/sst_chunked.zarr"

    main(source_path, store_path)
