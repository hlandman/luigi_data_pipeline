import os
import pandas as pd
import numpy as np

from luigi import *
from luigi.contrib.s3 import S3Target
from pset_utils.luigi.dask.target import *


class GetBadData(ExternalTask):
    """Reads file from S3 or Local source
    File must have column names
    :param
    """

    # Enter root S3 or local path, as a constant.
    # Full file path for S3, just directory for local
    DATA_ROOT = 'data\\unclean\\'

    # Unclean file's local name as luigi parameter
    filename = Parameter('customer_data_duped.csv')

    # Specify if "S3" or "Local"
    source_type = Parameter(default="S3")

    # Specify output type "dask" or "pandas"
    output_type = Parameter(default="pandas")

    def output(self):
        # Output depends on S3 and Local parameters
        if self.source_type == "S3":
            # Return S3 Target
            return S3Target(self.DATA_ROOT)
        elif self.source_type == "Local":
            # Returns Target type based on Dask or Pandas
            if self.output_type == "pandas" or "dask":
                # Returns CSVTarget
                return CSVTarget(self.DATA_ROOT, glob='*.csv', flag='')
            else:
                raise NotImplementedError
        else:
            # NotImplementedError for anything other than S3 or Local source
            print("Please use source_type 'S3' or 'Local.' Other types not implemented")
            raise NotImplementedError


class SaveCSVLocally(Task):
    """Saves the S3Target Locally"""

    # Destination directory for unlcean file
    DATA_DEST = 'data\\unclean\\'

    # Filename parameter from upstream task
    filename = GetBadData().filename
    output_type = GetBadData().output_type

    def requires(self):
        return GetBadData()

    def output(self):
        # Returns Target type based on Dask or Pandas
        if self.output_type == "pandas" or "dask":
            # Returns CSVTarget
            return CSVTarget(self.DATA_DEST, glob='*.csv', flag='')
        else:
            raise NotImplementedError

    def run(self):

        with self.input().open('r') as infile, self.output().open('w') as out_file:
            out_file.write(infile.read())


class DataCleaner(Task):
    """Cleans Data"""

    CLEAN_PATH = 'data\\cleaned\\'
    source_type = Parameter(default="Local")
    output_type = GetBadData().output_type
    filename = Parameter(GetBadData().filename)
    has_column_names = BoolParameter(default=True)

    def requires(self):
        if self.source_type == "S3":
            return SaveCSVLocally()
        elif self.source_type == "Local":
            return GetBadData()
        else:
            raise NotImplementedError

    def output(self):
        # Returns Target type based on Dask or Pandas
        if self.output_type == "pandas" or "dask":
            # Returns CSVTarget
            return CSVTarget(path=self.CLEAN_PATH)
        else:
            raise NotImplementedError

    def run(self):
        if self.has_column_names:
            df = self.input().read_dask(encoding='unicode_escape')
            print(df.head())
            self.output().write_dask(df, compression='gzip')
        else:
            raise NotImplementedError

