import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn import preprocessing
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

from luigi import *
from luigi.contrib.s3 import S3Target
from pset_utils.luigi.dask.target import *


class GetBadData(ExternalTask):
    """Reads file from S3 or Local source
    File must be CSV and have column indexes.
    Store local files in data/unclean at top of repository.
    Luigi Parameters (optional):
        filename: Name of output file as string
        source_type: "S3" or "Local"
    """

    # Enter root S3 or local path, as a constant.
    # Full file path for S3, just directory for local
    DATA_ROOT = 'data\\unclean\\'

    # Unclean file's local name as luigi parameter
    filename = Parameter(default='clean_file.csv')

    # Specify if "S3" or "Local"
    source_type = Parameter(default="Local")

    def output(self):
        # Output depends on S3 and Local parameters
        if self.source_type == "S3":
            # Return S3 Target
            return S3Target(self.DATA_ROOT)
        elif self.source_type == "Local":
            # Returns CSVTarget
            return CSVTarget(self.DATA_ROOT, glob='*.csv', flag='')
        else:
            # NotImplementedError for anything other than S3 or Local source
            print("Please use source_type 'S3' or 'Local.' Other types not implemented")
            raise NotImplementedError


class SaveCSVLocally(Task):
    """Saves the S3Target Locally
    Only used when pulling from S3.
    """

    # Destination directory for unlcean file
    DATA_DEST = 'data\\unclean\\'

    # Filename parameter from upstream task
    filename = GetBadData().filename
    output_type = GetBadData().output_type

    def requires(self):
        return GetBadData()

    def output(self):
        # Returns CSVTarget
        return CSVTarget(self.DATA_DEST, glob='*.csv', flag='')

    def run(self):

        with self.input().open('r') as infile, self.output().open('w') as out_file:
            out_file.write(infile.read())


class DataCleaner(Task):
    """Cleans Data
    Luigi Parameters (optional):
        Source
            source_type: string default="Local", or "S3"
        Date Parsing
            date_data: bool default=False, include --date_data in call to parse dates
            date_column: string default="date", or name of date column
        Missing Values
            drop_nan: string default="none", or "rows," "columns," or "both" for what to drop if there are na's
            na_filler: string default=' ', or what to fill na's with
    """

    # Folder for clean data
    CLEAN_PATH = os.path.join('data', 'cleaned\\')

    # Inherits filename from GetBadData
    filename = Parameter(GetBadData().filename)

    # Parameter for having indexes - False will return NotImplementedError
    has_column_names = BoolParameter(default=True)

    # Type of source, Local or S3
    source_type = Parameter(default="Local")

    # Parse Dates - True if there is a date column to parse
    date_data = BoolParameter()
    # Name of date column"
    date_column = Parameter(default="date")

    # Drop "rows," "columns," "both" or "none"
    drop_nan = Parameter(default="none")
    # Fill na - what to fill with
    na_filler = Parameter(default=' ')

    def requires(self):
        # Uses SaveCSVLocally as upstream if using S3 source
        if self.source_type == "S3":
            return SaveCSVLocally()
        # Uses GetBadData as upstream if using Local source
        elif self.source_type == "Local":
            return GetBadData()
        else:
            raise NotImplementedError

    def output(self):
        # Returns Local Target
        return LocalTarget(path=str(self.CLEAN_PATH) + str(self.filename))

    def run(self):
        if self.has_column_names:
            # Deal with nonstandard missing values
            missing_values = ["n/a", "na", "--"]

            if self.date_data:
                # Read in the Target, parsing dates
                df = self.input().read_dask(parse_dates=[self.date_column], na_values=missing_values,
                                            encoding='unicode_escape')
            else:
                # Read in the Target without parsing dates
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

            # Output to CSV file in "Cleaned" folder
            outdir = self.CLEAN_PATH
            if not os.path.exists(outdir):
                os.mkdir(outdir)
            df.to_csv(os.path.join(outdir, str(GetBadData().filename)))

        else:
            # Raises NotImplementedError if has_column_names returns False
            print("Please use data with column indexes. Index-less data not implemented.")
            raise NotImplementedError


class DataEncoder(Task):
    """Encodes variables
    Functions if encoder Param is specified in DataVisualizer.
    Luigi Parameter (required):
        cols: In command line call, need to specify as dict which columns to encode.
            Example: --cols '{"cat": "some_categorical", "dum": "some_dummy"}'
    """

    # Path to encoded file
    ENCODED_PATH = os.path.join('data', 'encoded\\')

    # Inherit filename from GetBadData
    filename = Parameter(GetBadData().filename)

    # DictParameter for which columns to encode
    cols = DictParameter(default={"cat": "none", "dum": "none"})

    def requires(self):
        return DataCleaner()

    def output(self):
        # Returns LocalTarget
        return LocalTarget(path=self.ENCODED_PATH + str(self.filename))

    def run(self):
        # Read in Target
        df = pd.read_csv(self.input().open('r'))

        # Encode categorical columns
        if "cat" in self.cols:
            if self.cols["cat"] != "none":
                l_encoder = preprocessing.LabelEncoder()
                df[self.cols["cat"]] = l_encoder.fit_transform(df[self.cols["cat"]])

        # Encode Dummy Variables
        if "dum" in self.cols:
            if self.cols["dum"] != "none":
                df[self.cols["dum"]] = pd.get_dummies(df[self.cols["dum"]])

        # Output to CSV file in "encoded" folder
        outdir = self.ENCODED_PATH
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        df.to_csv(os.path.join(outdir, str(self.filename)))


class DataVisualizer(Task):
    """Visualizes Data in a Customizable Way
    Luigi Parameters:
        Data (optional)
            figure_name: string default='figure.pdf', or figure output name
            encoder: bool default=False, include if you need to encode before visualizing

        Chart (optional)
            func: string default="lm" for lmplot, or "cat" for catplot, "facet" for FacetGrid, "pair" for PairGrid
            kind: string default="point," or any catplot kind

        Chart (required)
            xyvars: In command line call, need to specify as dict which x and y variables to plot.
                Example: --xyvars'{'x': 'some_x', 'y': 'some_y'}'
    """

    # Path to visual figure
    VISUAL_PATH = os.path.join('data', 'visualized\\')

    # Figure file name parameter
    figure_name = Parameter(default='figure.pdf')

    # Do you need Encoding?
    encoder = BoolParameter()

    # Specify "func": "lm" for lmplot, "cat" for catplot, "facet" for FacetGrid, "pair" for PairGrid
    func = Parameter(default="lm")

    # Optional - specify "kind": for catplot kind
    kind = Parameter(default="point")

    # Specify "x" and "y" variables as dict
    xyvars = DictParameter()

    def requires(self):
        if self.encoder:
            return DataEncoder()
        else:
            return DataCleaner()

    def output(self):
        # Returns Local Target
        return LocalTarget(path=self.VISUAL_PATH + str(self.figure_name))

    def run(self):
        # Read in data frame
        df = pd.read_csv(self.input().open('r'))

        # If FacetGrid parameter is specified
        if self.func == "facet":
            g = sns.FacetGrid(df, row=self.xyvars["x"], col=self.xyvars["y"], margin_titles=True)
            g.map(self.kind, self.xyvars["y"], hist=False, rug=False)

        # If PairGrid parameter is specified
        elif self.func == "pair":
            g = sns.PairGrid(df, diag_sharey=False)
            g.map(self.kind)

        # If lmplot is specified
        elif self.func == "lm":
            # Ensure 'x' and 'y' vars are specified in "xyvar" parameter Dict
            if "x" and "y" in self.xyvars:
                g = sns.lmplot(x=self.xyvars["x"], y=self.xyvars["y"], data=df)
            else:
                print("Please provide X and Y variables for the 'xyvars' Parameter in the form of a Dict. "
                      "Example: --xyvars'{'x': 'some_x', 'y': 'some_y'}'")
                raise NotImplementedError

        # If catplot is specified
        elif self.func == "cat":
            # Ensure 'x' and 'y' vars are specified in "xyvar" parameter Dict
            if "x" and "y" in self.xyvars:
                g = sns.catplot(x=self.xyvars["x"], y=self.xyvars["y"], data=df, kind=self.kind)
            else:
                print("Please provide X and Y variables for the 'xyvars' Parameter in the form of a Dict. "
                      "Example: --xyvars'{'x': 'some_x', 'y': 'some_y'}'")
                raise NotImplementedError
        else:
            raise NotImplementedError

        # Output to PDF file in "visualized" folder
        outdir = self.VISUAL_PATH
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        g.savefig(os.path.join(outdir, str(self.figure_name)))
