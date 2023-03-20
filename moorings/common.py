# Common functions used by moorings notebooks
import re

import pandas as pd
from owslib.etree import etree
from owslib.fes import PropertyIsEqualTo, PropertyIsNotEqualTo, PropertyIsLike, And
from owslib.wfs import WebFeatureService

# Set up WFS connection
WFS_URL = "http://geoserver-123.aodn.org.au/geoserver/wfs"
WFS_VERSION = '1.1.0'
INDEX_LAYER = 'imos:anmn_all_map'
WFS = WebFeatureService(WFS_URL, version=WFS_VERSION)


def file_index_df(site_code_pattern: str = None) -> pd.DataFrame:
    """Get an index of all ANMN FV01 timeseries files from AODN geoserver and return as
     a Pandas DataFrame.

    A description of the index layer can be found here:
    https://github.com/aodn/anmn-internal-discussions/wiki/Getting-information-on-published-files#information-about-published-moorings-files
    """
    filter_list = [
        PropertyIsEqualTo(propertyname='file_version', literal='1'),
        PropertyIsEqualTo(propertyname='realtime', literal='false'),
        PropertyIsNotEqualTo(propertyname='data_category', literal='Biogeochem_profiles'),
        PropertyIsNotEqualTo(propertyname='data_category', literal='CTD_profiles'),
        PropertyIsNotEqualTo(propertyname='data_category', literal='CO2'),
        PropertyIsNotEqualTo(propertyname='data_category', literal='aggregated_timeseries')
    ]
    if site_code_pattern is not None:
        filter_list.append(PropertyIsLike(propertyname='site_code', literal=site_code_pattern))
    filter = etree.tostring(And(filter_list).toXML(), encoding='unicode')
    propertyname = ['site_code', 'deployment_code', 'data_category',
                    'instrument', 'instrument_nominal_depth', 'variables',
                    'time_deployment_start', 'time_deployment_end'
                    ]
    with WFS.getfeature(typename=INDEX_LAYER, propertyname=propertyname, filter=filter, outputFormat='csv') as response:
        df = pd.read_csv(response, parse_dates=['time_deployment_start', 'time_deployment_end'])

    # drop unused FID column
    df.drop(columns='FID', inplace=True)

    # convert variables to a list
    df['variables'] = df['variables'].map(lambda s: s.split(", "))

    return df


def pivot_variables(df: pd.DataFrame, var_set: set) -> pd.DataFrame:
    """Rearrange the file-list data frame so that each row lists one variable only
     (multiple rows per file where needed), filtering out only the variables
     in var_set.
    """
    assert 'variables' in df.columns
    files_vars = []
    for row in df.itertuples():
        variables = set(row.variables) & var_set
        files_vars.extend((row.Index, v) for v in variables)

    files_vars = pd.DataFrame(files_vars, columns=['i', 'variable']).set_index('i')

    return df.drop(columns='variables').join(files_vars)
