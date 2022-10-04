import concurrent.futures

import s3fs
import xarray as xr


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


def get_paths(source_path):
    s3 = s3fs.S3FileSystem(anon=False)
    glob_result = s3.glob(source_path)

    paths = ["s3://" + path for path in glob_result]
    return paths


def read_clean_data(s3_path: str) -> xr.Dataset:
    s3 = s3fs.S3FileSystem(anon=False)
    data = xr.open_dataset(s3.open(s3_path), engine="h5netcdf")
    available_vars = set(data.variables)
    to_drop_vars = list(available_vars - CONSISTENT_VARS)
    data = data.drop_vars(to_drop_vars)

    return data


def process_one_file(arguments):
    path, store_path, create = arguments

    print(f"Working on {path}")

    try:
        data = read_clean_data(path)
        data = data.chunk({"time": 1, "lon": 500, "lat": 500})
        if create:
            data.to_zarr(store_path, mode="w", consolidated=True)
        else:
            data.to_zarr(store_path, mode="a", consolidated=True, append_dim="time")
    except Exception as e:
        print(f"Failed {path} with exception {e}")
        return False

    print(f"Finished {path}")
    return True


def main(source_path, store_path):
    paths = get_paths(source_path)

    # TODO: check if the zarr exists first, and only then create it
    path = paths.pop(0)
    print("Creating template file...")
    process_one_file((path, store_path, True))

    paths = paths[0:13]

    # Process each file in the list on a separate thread
    print(f"Processing {len(paths)} files")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(process_one_file, [(p, store_path, False) for p in paths])

    failed = not all(results)
    if failed:
        print("Failed to write all files...")


if __name__ == "__main__":
    source_path = "s3://imos-data/IMOS/SRS/SST/ghrsst/L3S-1d/day/2022/*.nc"
    store_path = "s3://imos-data-pixeldrill/alexl/sst.zarr/"

    main(source_path, store_path)
