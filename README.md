[![Build Status](https://travis-ci.com/csci-e-29/2019sp-final-project-panic42station.svg?token=Z1zVMfxDc9bFJyjazoVH&branch=master)](https://travis-ci.com/csci-e-29/2019sp-final-project-panic42station)

# Final Project - Luigi Data Cleaner for R

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Before you begin](#before-you-begin)
- [Problems (45 points)](#problems-45-points)
  - [Data](#Data)
  - [Composition (20 points)](#composition-20-points)
    - [Requires and Requirement (10 points)](#requires-and-requirement-10-points)
    - [TargetOutput (10 points)](#targetoutput-10-points)
      - [Salted (Optional)](#salted-optional)
  - [Dask Targets (10 points)](#dask-targets-10-points)
  - [Dask Analysis (15 points)](#dask-analysis-15-points)
    - [Yelp Reviews](#yelp-reviews)
    - [Clean the data (5 points)](#clean-the-data-5-points)
    - [Analysis (10 points)](#analysis-10-points)
      - [CLI (5 points)](#cli-5-points)
      - [ByDecade](#bydecade)
      - [ByStars](#bystars)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Before you begin

### Initial Setup
Begin with your cookiecutter template as usual, and manaully merge and link
to this repo as origin.

### Getting the Data
Download any unclean dataset in CSV format. One example can be found [here](https://assets.datacamp.com/production/repositories/377/datasets/772b2f7aeec588ada8654ff7be744c4e5cd426c4/attendance.xls).
Store the csv in a folder called "data/" at the top of your repository.

#### Optional: Get data from S3
Instead of downloading data locally, pull data from S3. You will be building in functionality to pull either locally
or from S3, but for the purpose of this pset you may pull the data from a local source.

## Problems (45 Points)
This pset will automate a data-cleaning process for use in R.

### Import Data (10 Points)
Create a luigi task that can pull csv data files either from local or S3 sources, based on parameters. 

### Testing Tasks

Testing Luigi tasks end to end can be hard, but not impossible. You can write
tests using fake tasks:

```python
from tempfile import TemporaryDirectory
from luigi import Task, build

class SaltedTests(TestCase):
    def test_salted_tasks(self):
        with TemporaryDirectory() as tmp:
            class SomeTask(Task):
                output = SaltedOutput(base_dir=tmp)
                ...

            # Decide how to test a salted workflow
```

Note that you can use `build([SomeTask()], local_scheduler=True)` inside a test
to fully run a luigi workflow, but you may want to suppress some warnings if you
do so.



### Composition (20 points)

Review the descriptor patterns to replace `def output(self):` and  `def
requires(self):` with composition.

Create a new package/module `pset_utils.luigi.task` to capture this work.

#### Requires and Requirement (10 points)

Finish the implementation of `Requires` and `Requirement` as discussed in
lecture.

```python
# pset_utils.luigi.task
class Requirement:
    def __init__(self, task_class, **params):
        ...

    def __get__(self, task, cls):
        return task.clone(
            self.task_class,
            **self.params)


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires`

    Example::

        class MyTask(Task):
            # Replace task.requires()
            requires = Requires()  
            other = Requirement(OtherTask)

            def run(self):
                # Convenient access here...
                with self.other.output().open('r') as f:
                    ...

        >>> MyTask().requires()
        {'other': OtherTask()}

    """

    def __get__(self, task, cls):
        if task is None:
            return self

        # Bind self/task in a closure
        return lambda : self(task)

    def __call__(self, task):
        """Returns the requirements of a task

        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.

        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        # Search task.__class__ for Requirement instances
        # return
        ...


class Requirement:
    def __init__(self, task_class, **params):
        ...

    def __get__(self, task, cls):
        if task is None:
            return self

        return task.clone(
            self.task_class,
            **self.params)
```

Tips:

* You can access all the properties of an object using `obj.__dict__` or
`dir(obj)`.  The latter will include inherited properties, while the former will
not.

* You can access the class of an object using `obj.__class__` or `type(obj)`.
  * eg, `MyTask().__class__ is MyTask`

* The properties on an instance are not necessarily the same as the properties
on a class, eg `other` is a `Requirement` instance when accessed from the class
`MyTask`, but should be an instance of `OtherTask` when accessed from an
instance of `MyTask`.

* You can get an arbitrary property of an object using `getattr(obj, 'asdf')`,
eg `{k: getattr(obj, k) for k in dir(obj)}`

* You can check the type using `isinstance(obj, Requirement)`

* You can use dict comprehensions eg `{k: v for k, v in otherdict.items() if
condition}`


#### TargetOutput (10 points)
Implement a descriptor that can be used to generate a luigi target using a
salted task id, inside the `task` module:

```python
# pset_utils.luigi.task
class TargetOutput:
    def __init__(self, file_pattern='{task.__class__.__name__}',
        ext='.txt', target_class=LocalTarget, **target_kwargs):
        ...

    def __get__(self, task, cls):
        if task is None:
            return self
        return lambda: self(task)

    def __call__(self, task):
        # Determine the path etc here
        ...
        return self.target_class(...)
```

Note the string patterns in file_pattern, which are intended to be used like
such:

```python
>>> '{task.param}-{var}.txt'.format(task=task, var='world')
'hello-world.txt'
```

See [str.format](https://docs.python.org/3.4/library/stdtypes.html#str.format)
for reference.

##### Salted (Optional)

This part is ***optional***.  You do not need to implement salted to receive
full credit, but you should play with it when you have time.

If you do not implement salted, you must still ensure your local answers are
correct before submitting them to canvas!

You can either create a subclass of `TargetOutput` or allow for this
functionality in the main class, eg:

```python
class SaltedOutput(TargetOutput):
    def __init__(self, file_pattern='{task.__class__.__name__}-{salt}',...):
        ...
```

If the format string asks for the keyword `salt`, you should calculate the
task's salted id as a hexdigest and include it as a kwarg in the string format.

Refer to `get_salted_id` in the demo repo to get the globally unique salted data
id.

### Dask Targets (10 points)

Dask outputs are typically folders; as such, they are not suitable for
directly using the luigi `FileSystemTarget` variants.  Dask uses its own file
system abstractions which are not compatible with Luigi's.

Correctly implementing the appropriate logic is a bit difficult, so you can
start with the included code.  Add it to `pset_utils.luigi.dask.target`:

```python
"""Luigi targets for dask collections"""

from dask import delayed
from dask.bytes.core import get_fs_token_paths
from dask.dataframe import read_csv, read_parquet, to_csv, to_parquet
from luigi import Target
from luigi.task import logger as luigi_logger

FLAG = "_SUCCESS"  # Task success flag, c.f. Spark, hadoop, etc


@delayed(pure=True)
def touch(path, storage_options=None, _dep=None):
    fs, token, paths = get_fs_token_paths(path, storage_options=storage_options)
    with fs.open(path, mode="wb"):
        pass


class BaseDaskTarget(Target):
    """Base target for dask collections

    The class provides reading and writing mechanisms to any filesystem
    supported by dask, as well as a standardized method of flagging a task as
    successfully completed (or checking it).

    These targets can be used even if Dask is not desired, as a way of handling
    paths and success flags, since the file structure (parquet/csvs/jsons etc)
    are identical to other Hadoop and alike systems.
    """

    def __init__(self, path, glob=None, flag=FLAG, storage_options=None):
        """

        :param str path: Directory the collection is stored in.  May be remote
            with the appropriate dask backend (ie s3fs)

        :param str flag: file under directory which indicates a successful
            completion, like a hadoop '_SUCCESS' flag.  If no flag is written,
            or another file cannot be used as a proxy, then the task can only
            assume a successful completion based on a file glob being
            representative, ie the directory exists or at least 1 file matching
            the glob. You must set the flag to '' or None to allow this fallback behavior.

        :param str glob: optional glob for files when reading only (or as a
            proxy for completeness) or to set the name for files to write for
            output types which require such.

        :param dict storage_options: used to create dask filesystem or passed on read/write
        """

        self.glob = glob
        self.path = path
        self.storage_options = storage_options or {}
        self.flag = flag

        if not path.endswith(self._get_sep()):
            raise ValueError("Must be a directory!")

    @property
    def fs(self):
        fs, token, paths = get_fs_token_paths(
            self.path, storage_options=self.storage_options
        )
        return fs

    def _get_sep(self):
        """The path seperator for the fs"""
        try:
            return self.fs.sep  # Set for local files etc
        except AttributeError:
            # Typical for s3, hdfs, etc
            return "/"

    def _join(self, *paths):
        sep = self._get_sep()
        pths = [p.rstrip(sep) for p in paths]
        return sep.join(pths)

    def _exists(self, path):
        """Check if some path or glob exists

        This should not be an implementation, yet the underlying FileSystem
        objects do not share an interface!

        :rtype: bool
        """

        try:
            _e = self.fs.exists
        except AttributeError:
            try:
                return len(self.fs.glob(path)) > 0
            except FileNotFoundError:
                return False
        return _e(path)

    def exists(self):
        # NB: w/o a flag, we cannot tell if a task is partially done or complete
        fs = self.fs
        if self.flag:
            ff = self._join(self.path, self.flag)
            if hasattr(fs, 'exists'):
                # Unfortunately, not every dask fs implemenents this!
                return fs.exists(ff)

        else:
            ff = self._join(self.path, self.glob or "")

        try:
            for _ in fs.glob(ff):
                # If a single file is found, assume exists
                # for loop in case glob is iterator, no need to consume all
                return True
        except FileNotFoundError:
            return False

    def mark_complete(self, _dep=None, compute=True):
        if not self.flag:
            raise RuntimeError("No flag for task, cannot mark complete")

        flagfile = self._join(self.path, self.flag)

        out = touch(flagfile, _dep=_dep)
        if compute:
            out.compute()
            return
        return out

    def augment_options(self, storage_options):
        """Get composite storage options

        :param dict storage_options: for a given call, these will take precedence

        :returns: options with defaults baked in
        :rtype: dict
        """
        base = self.storage_options.copy()
        if storage_options is not None:
            base.update(storage_options)
        return base

    def read_dask(self, storage_options=None, check_complete=True, **kwargs):
        if check_complete and self.flag and not self.exists():
            raise FileNotFoundError("Task not yet run or incomplete")

        return self._read(
            self.get_path_for_read(),
            storage_options=self.augment_options(storage_options),
            **kwargs
        )

    def get_path_for_read(self):
        if self.glob:
            return self._join(self.path, self.glob)
        return self.path.rstrip(self._get_sep())

    def get_path_for_write(self):
        return self.path.rstrip(self._get_sep())

    def write_dask(
        self,
        collection,
        compute=True,
        storage_options=None,
        logger=luigi_logger,
        **kwargs
    ):
        if logger:
            logger.info("Writing dask collection to {}".format(self.path))
        storage_options = self.augment_options(storage_options)
        out = self._write(
            collection,
            self.get_path_for_write(),
            storage_options=storage_options,
            compute=False,
            **kwargs
        )

        if self.flag:
            out = self.mark_complete(_dep=out, compute=False)

        if compute:
            out.compute()
            if logger:
                logger.info(
                    "Successfully wrote to {}, flagging complete".format(self.path)
                )
            return None
        return out

    # Abstract interface: implement for various storage formats

    @classmethod
    def _read(cls, path, **kwargs):
        raise NotImplementedError()

    @classmethod
    def _write(cls, collection, path, **kwargs):
        raise NotImplementedError()


class ParquetTarget(BaseDaskTarget):
    ...


class CSVTarget(BaseDaskTarget):
    ...
```

### Dask Analysis (15 points)
Implement your luigi tasks in `pset_5.tasks`

#### Yelp Reviews
The data for this problem set is here:
```bash
$ aws s3 ls s3://cscie29-data/pset5/yelp_data/
2019-03-29 16:35:53    6909298 yelp_subset_0.csv
...
2019-03-29 16:35:54    7010664 yelp_subset_19.csv
```
***Do not copy the data! Everything will be done with dask.***

Write an ExternalTask named `YelpReviews` which uses the appropriate dask
target.

#### Clean the data (5 points)
We will turn the data into a parquet data set and cache it locally.  Start with
something like:

```python
class CleanedReviews(Task):
    subset = BoolParameter(default=True)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    def run(self):

        numcols = ["funny", "cool", "useful", "stars"]
        dsk = self.input().read_dask(...)

        if self.subset:
            dsk = dsk.get_partition(0)

        out = ...
        self.output().write_dask(out, compression='gzip')
```

Note that we turn on `subset` by default to limit bandwidth.  You will run the
full dataset from your computer only and only to provide the direct answers
to the quiz.

Notes on cleaning:

* All computation should use Dask.  Do not compute to a pandas DF (you may use
map_partitions etc)

* Ensure that the `date` column is parsed as a pandas datetime using
[parse_dates](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)

* The columns `["funny", "cool", "useful", "stars"]` are all inherently
integers. However, given there are missing values, you must first read them as
floats, fill nan's as 0, then convert to int.  You can provide a dict of
`{col: dtype}` when providing the dtype arg in places like `read_parquet` and
`astype`

* There are about 60 rows (out of 200,000) that are corrupted which you can
drop. They are misaligned, eg the text shows up in the row id.  You can find
them by dropping columns where `user_id` is null or the length of `review_id`
is not 22.

* You should set the index to `review_id` and ensure the output reads back with
meaningful divisions

#### Analysis (10 points)
Let's look at how the length of a review depends on some factors.  By length,
let's just measure the number of characters (no need for text parsing).

##### CLI (5 points)
Running `python -m pset_5` on your master branch in Travis should display the
results of each of these ***on the subset of data***.

You should add a flag, eg `python -m pset_5 --full` to run and display the
results on the entire set.  Use these for your quiz responses locally.

The tasks should run and read back their results for printing, eg:

```python
class BySomething(Task):

    # Be sure to read from CleanedReviews locally

    def run(self):
        ...

    def print_results(self):
        print(self.output().read_dask().compute())
```

You can restructure using parent classes if you wish to capture some of the
patterns.

A few tips:

* You may need to use the `write_index` param in `to_parquet` after a `groupby`
given the default dask behavior. See
[to_parquet](http://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.to_parquet)

* You cannot write a `Series` to parquet. If you have something like
`df['col'].groupby(...).mean()` then you need to promote it back to a dataframe,
like `series.to_frame()`, or `df[['col']]` to keep it as a 1-column frame to
begin with.

* Round all your answers and store them as integers.  Report the integers in
Canvas.

* Consider using pandas/dask [text
tools](https://pandas.pydata.org/pandas-docs/stable/user_guide/text.html) like
`df.str`

* Only load the columns you need from `CleanedReviews`!  No need to load all
columns every time.

##### ByDecade

What is the average length of a review by the decade (eg `2000 <= year < 2010`)?

##### ByStars
What is the average length of a review by the number of stars?
