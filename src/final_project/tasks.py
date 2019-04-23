import os
import pandas as pd
import numpy as np

from luigi import *
from pset_utils.luigi.dask.target import *


class GetBadData(ExternalTask):
    """Reads file from internet source"""

    # Root path, as a constant
    DATA_ROOT = 'https://assets.datacamp.com/production/repositories/377/datasets/772b2f7aeec588ada8654ff7be744c4e5cd426c4/attendance.xls'

    # Unclean file's local name as luigi parameter
    filename = Parameter('unclean.csv')

    def output(self):
        # Return a CSVTarget
        return CSVTarget(self.DATA_ROOT, glob='*.csv', flag='')


class SaveCSVLocally(Task):
    """Saves the CSVTarget Locally"""

    # Destination directory for unlcean file
    DATA_DEST = os.path.join('data', 'unclean')

    # Filename parameter from upstream task
    filename = GetBadData().filename

    def requires(self):
        return GetBadData()

    def output(self):
        return LocalTarget(self.DATA_DEST + '\\' + self.filename)


class DataCleaner(Task):
    """Cleans Data"""

    def requires(self):
        return SaveCSVLocally()

    def output(self):
        return LocalTarget(path='data\\cleaned\\')

    def run(self):


