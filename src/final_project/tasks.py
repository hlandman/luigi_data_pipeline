import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt

from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

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
    filename = Parameter('winequality-red.csv')

    # Specify if "S3" or "Local"
    source_type = Parameter(default="Local")

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
    """Cleans and Wrangles Data
        Luigi Parameters:
        source_type: string "Local" or "S3"
        date_column: string "False" if none, else string date column name
        drop_nan: string "rows," "columns," "both" or "none" for what to drop if there are na's
        na_filler: string what to fill na's with
        category_col: string "none" or name of categorical column for encoding variables
        dummy_col: string "none" or name of dummy variable column
    """

    CLEAN_PATH = os.path.join('data', 'cleaned\\')
    output_type = GetBadData().output_type
    filename = Parameter(GetBadData().filename)
    has_column_names = BoolParameter(default=True)

    # Type of source, Local or S3
    source_type = Parameter(default="Local")

    # Parse Dates - False if no date column, column name if there is
    date_data = BoolParameter(default=False)
    date_column = Parameter(default="date")

    # Drop "rows," "columns," "both" or "none"
    drop_nan = Parameter(default="none")
    na_filler = Parameter(default=' ')

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
            # Returns Target
            return LocalTarget(path=str(self.CLEAN_PATH) + str(self.filename))
        else:
            raise NotImplementedError

    def run(self):
        if self.has_column_names:
            # Deal with nonstandard missing values
            missing_values = ["n/a", "na", "--"]

            if self.date_data:
                # Read in the Target
                df = self.input().read_dask(parse_dates=[self.date_column], na_values=missing_values,
                                            encoding='unicode_escape')
            else:
                # Read in the Target
                df = self.input().read_dask(na_values=missing_values, encoding='unicode_escape')

            df = df.compute()

            # Fill or drop NaN based on parameter
            if self.drop_nan == "rows":
                df.dropna()
            elif self.drop_nan == "columns":
                df.dropna(axis=1)
            elif self.drop_nan == "both":
                df.dropna()
                df.dropna(axis=1)
            else:
                df.fillna(self.na_filler)

            print(df.head())

            # Output to CSV file in "Cleaned" folder
            outdir = self.CLEAN_PATH
            if not os.path.exists(outdir):
                os.mkdir(outdir)
            df.to_csv(os.path.join(outdir, str(GetBadData().filename)))

            # self.output().to_csv(df, compression='gzip')

        else:
            raise NotImplementedError


class DataEncoder(Task):
    """Encodes variables"""
    ENCODED_PATH = os.path.join('data', 'encoded\\')

    filename = Parameter(GetBadData().filename)

    # Encode Categorical Columns - "none" or column name
    # Multiple columns??
    category_col = Parameter("quality")

    # Dummy Columns - "none" or column name
    dummy_col = Parameter("good")

    def requires(self):
        return DataCleaner()

    def output(self):
        # Returns CSV Target
        return LocalTarget(path=self.ENCODED_PATH + str(self.filename))

    def run(self):
        df = pd.read_csv(self.input().open('r'))

        # Encode labels
        if self.category_col != "none":
            l_encoder = preprocessing.LabelEncoder()
            df[self.category_col] = l_encoder.fit_transform(df[self.category_col])

        # Dummy Variables
        if self.dummy_col != "none":
            # hot_encoder = OneHotEncoder(handle_unknown='ignore')

            df[self.dummy_col] = pd.get_dummies(df[self.dummy_col])

        print(df.head())

        # Output to CSV file in "Cleaned" folder
        outdir = self.ENCODED_PATH
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        df.to_csv(os.path.join(outdir, str(self.filename)))


class DataVisualizer(Task):
    """Visualizes Data - Customizably"""
    VISUAL_PATH = os.path.join('data', 'visualized\\')

    figure_name = Parameter('figure.pdf')

    # Do you need Encoding?
    encoder = BoolParameter(default=True)

    # Specify what type of chart - "scatter," "line," "hist," "bar"
    chart_type = Parameter(default="scatter")
    chart_title = Parameter(default="My Chart")

    # Choose column name for bar chart
    bar_col = Parameter(default="none")

    # Pass in column name for x and y axes
    x_axis = Parameter(default="sulphates")
    y_axis = Parameter(default="quality")

    def requires(self):
        if self.encoder:
            return DataEncoder()
        else:
            return DataCleaner()

    def output(self):
        # Returns Local Target
        return LocalTarget(path=self.VISUAL_PATH + str(self.figure_name))

    def run(self):
        df = pd.read_csv(self.input().open('r'))

        if self.chart_type == "scatter":
            chart = df.plot.scatter(x=self.x_axis, y=self.y_axis, title=self.chart_title)
        elif self.chart_type == "line":
            chart = df.plot.line(title=self.chart_title)
        elif self.chart_type == "hist":
            chart = df.plot.hist(subplots=True, layout=(2, 2), figsize=(10, 10), bins=20)
        # elif self.chart_type == "bar":
        else:
            chart = df[self.bar_col].value_counts().sort_index().plot.bar()

        fig = chart.get_figure()

        # Output to CSV file in "Cleaned" folder
        outdir = self.VISUAL_PATH
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        fig.savefig(os.path.join(outdir, str(self.figure_name)))
        # df.to_csv(os.path.join(outdir, str(self.figure_name)))
