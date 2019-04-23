import pandas as pd
import numpy as np
from luigi import *
from pset_utils.luigi.dask.target import *


class GetBadData(ExternalTask):
    """Reads file from internet source"""

    # Root path, as a constant
    DATA_ROOT = 'https://assets.datacamp.com/production/repositories/377/datasets/772b2f7aeec588ada8654ff7be744c4e5cd426c4/attendance.xls'

    def output(self):
        # Return a LocalTarget
        return CSVTarget(self.DATA_ROOT, glob='*.csv', flag='')


class DataCleaner(Task):
    """Cleans Data"""

    def requires(self):
        return GetBadData()

    def output(self):
        return LocalTarget(path='data\\')

    def run(self):


