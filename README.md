# aodn-public-notebooks

To run the notebooks, you will need to install required dependencies `pip install -r requirements.txt`, it's recommended to use virual Python environment tools such as `Conda`.

```bash
conda install -n <your_env_name> python=<version> ipython --yes
```
Add your env to Jupyter Lab:

```bash
python -m ipykernel install --user --name <your_env_name> --display-name "<name_you_want>"
```

Below is a list of dependencies used for multi-purpose environment for the time being, feel free to remove the packages that may not be required in your scope of usage:

```
setuptools
xarray[complete]
tornado
jinja2==3.0.3
bokeh==2.4.3
boto3==1.20.24
botocore==1.23.24
jsonschema==4.7.2
toolz==0.11.2
git+https://github.com/intake/kerchunk.git@0.0.5
cloudpickle==2.0.0
dask==2022.1.1
distributed==2022.1.1
msgpack==1.0.2
toolz==0.11.2
pandas==1.3.4
numpy==1.21.5
graphviz==0.8.1
click==8.0.4
dask-cloudprovider==2022.1.0
jedi==0.18.1
s3fs==2022.1.0
fsspec==2022.1.0
h5netcdf==0.13.1
numcodecs==0.9.1
blosc==1.10.6
lz4==3.1.10
pickle5==0.0.11
retry==0.9.2
pyyaml==6.0
seawater==3.3.4
awswrangler==2.14.0
netCDF4==1.6.0
zarr==2.11.0
aioboto3==9.3.1
aiobotocore[boto3]==2.1.0
urllib3==1.26.7
```

If running Conda env, installing `eccodes`: `conda install -c conda-forge -y eccodes` fixes some library import warnings.

Also with `Conda`, if facing GDAL related errors during `pip` installing dependencies, run `conda uninstall gdal` then install again `conda install gdal` will help.

Our `imos-data-pixeldrill` is publicly available, sample method to access our datasets:

```python
import s3fs
s3 = s3fs.S3FileSystem(anon=True)
store_path = 's3://imos-data-pixeldrill/csiro-argo.zarr/'
store = s3fs.S3Map(root=store_path, s3=s3)
data = xr.open_zarr(store)
```

