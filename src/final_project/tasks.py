import os
import pandas as pd
import numpy as np

from luigi import *
from luigi.contrib.s3 import S3Target
from pset_utils.luigi.dask.target import *


class GetBadData(ExternalTask):
    """Reads file from S3 or Local source"""

    # Enter root S3 or local path, as a constant.
    # Full file path for S3, just directory for local
    DATA_ROOT = 's3://hillellandman/attendance.csv'

    # Unclean file's local name as luigi parameter
    filename = Parameter('unclean.csv')

    # Specify if S3 or Local
    S3 = BoolParameter(default=True)
    Local = BoolParameter(default=False)

    def output(self):
        # Output depends on S3 and Local parameters
        if self.S3:
            # Return S3 Target
            return S3Target(self.DATA_ROOT)
        elif self.Local:
            # Return a CSVTarget
            return CSVTarget(self.DATA_ROOT, glob='*.csv', flag='')
        else:
            # NotImplementedError for anything other than S3 or Local source
            raise NotImplementedError


class SaveCSVLocally(Task):
    """Saves the CSVTarget or S3Target Locally"""

    # Destination directory for unlcean file
    DATA_DEST = os.path.join('data', 'unclean')

    # Filename parameter from upstream task
    filename = GetBadData().filename

    def requires(self):
        return GetBadData()

    def output(self):
        return LocalTarget(self.DATA_DEST + '\\' + self.filename)

    def run(self):

        with self.input().open('r') as infile, self.output().open('w') as out_file:
            out_file.write(infile.read())


# class DataCleaner(Task):
#    """Cleans Data"""

#    S3 = BoolParameter(GetBadData().S3)
#    Local = BoolParameter(GetBadData().Local)

#    def requires(self):
#        if self.S3:
#           return SaveCSVLocally()
#        elif self.Local:
#           return GetBadData()
#        else:
#           raise NotImplementedError

#    def output(self):
#        return LocalTarget(path='data\\cleaned\\')

#    def run(self):


