import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn import preprocessing

from luigi import *
from pset_utils.luigi.dask.target import CSVTarget


class GetBadData(ExternalTask):
    """Reads file from Local source
    File must be CSV and have column indexes.
    Store local files in data/unclean at top of repository.
    Luigi Parameters (optional):
        filename: Name of output file as string
    """
    # Directory for local file
    DATA_ROOT = 'data\\unclean\\'

    # Unclean file's local name as luigi parameter
    filename = Parameter(default='clean_file.csv')

    def output(self):
        # Returns CSVTarget
        return CSVTarget(self.DATA_ROOT, glob='*.csv', flag='')


class DataCleaner(Task):
    """Cleans Data
    Luigi Parameters (optional):
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

    # Parse Dates - True if there is a date column to parse
    date_data = BoolParameter()
    # Name of date column"
    date_column = Parameter(default="date")

    # Drop "rows," "columns," "both" or "none"
    drop_nan = Parameter(default="none")
    # Fill na - what to fill with
    na_filler = Parameter(default=' ')

    def requires(self):
        return GetBadData()

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
                df = df.dropna()
            elif self.drop_nan == "columns":
                df = df.dropna(axis=1)
            elif self.drop_nan == "both":
                df = df.dropna()
                df = df.dropna(axis=1)
            else:
                df = df.fillna(value=self.na_filler)

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
    # Options fot cat are: “point”, “bar”, “strip”, “swarm”, “box”, “violin”, or “boxen”.
    kind = Parameter(default="point")

    # Specify "x" and "y" variables as dict
    # For Facet, specify "z" that you want to Facet
    xyvars = DictParameter(default={"x": "x", "y": "y"})

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
            g = sns.FacetGrid(df, col=self.xyvars["facet"], margin_titles=True)
            if self.kind == "scatter":
                g.map(plt.scatter, self.xyvars["x"], self.xyvars["y"])
            elif self.kind == "bar":
                g.map(sns.barplot, self.xyvars["x"], self.xyvars["y"])
            elif self.kind == "hist":
                g.map(plt.hist, self.xyvars["x"])
            elif self.kind == "point":
                g.map(sns.pointplot, self.xyvars["x"], self.xyvars["y"])
            elif self.kind == "dist":
                g.map(sns.distplot, self.xyvars["x"], self.xyvars["y"])
            else:
                print("Please use one of kind 'scatter,' 'bar,' 'hist,' 'point' or 'dist'"
                      "Others not implemented.")

        # If PairGrid parameter is specified
        elif self.func == "pair":
            g = sns.PairGrid(df, diag_sharey=False)
            if self.kind == "scatter":
                g.map(plt.scatter)
            elif self.kind == "bar":
                g.map(sns.barplot)
            elif self.kind == "hist":
                g.map(plt.hist)
            elif self.kind == "point":
                g.map(sns.pointplot)
            elif self.kind == "dist":
                g.map(sns.distplot)
            else:
                print("Please use one of kind 'scatter,' 'bar,' 'hist,' 'point' or 'dist'"
                      "Others not implemented.")
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
