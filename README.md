[![Build Status](https://travis-ci.com/csci-e-29/2019sp-final-project-panic42station.svg?token=Z1zVMfxDc9bFJyjazoVH&branch=master)](https://travis-ci.com/csci-e-29/2019sp-final-project-panic42station)

# Luigi Workflow for Customizable Data Cleaning, Encoding and Visualization

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Initial Setup](#initial-setup)
- [Functionality](#functionality)
  - [GetBadData](#getbaddataexternaltask)
  - [DataCleaner](#datacleanertask)
  - [DataEncoder](#dataencodertask)
  - [DataVisualizer](#datavisualizertask)
- [Parameter Table](#parameter-table)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Initial Setup

### Cookiecutter
Begin with your cookiecutter template as usual, and manaully merge and link
to this repo as origin.

### Environment
Pipenv install packages that we will use:
```bash
$ pipenv install luigi, pandas, numpy, seaborn
```

### Getting Data

Store a csv file in a folder called "data/unclean/" at the top of your repository.



***

## Functionality

This workflow functions as a Luigi pipeline under src/final_project/tasks.py

### GetBadData(ExternalTask)
Reads a local csv file, outputs to CSVTarget.

#### Luigi Parameters
###### filename 
_Luigi Parameter, default='clean_file.csv'_  
Use to change name of output file.  
    
Example call:
```bash
$ luigi --module final_project.tasks GetBadData --local-scheduler --filename <new_filename>
```

### DataCleaner(Task)
Requires: GetBadData  
Output: LocalTarget - saves csv file under 'data/cleaned/'  
Run: Cleans data. Can pass in parameters to parse dates and for how to deal with missing values.


#### Luigi Parameters
##### Date Parsing
###### date_data
_Luigi BoolParameter, default=False_   
Use to indicate whether or not your dataset has date data that needs parsing.

###### date_column

_Luigi Parameter, default='date'_  
Use to indicate the index name of your date column

##### Missing Values
###### drop_nan
_Luigi Parameter, default='none'_  
Use to determine if and when to drop missing data  
Options to pass in are 'none,' 'rows,' 'columns,' or 'both'

###### na_filler
_Luigi Parameter, default=' '_  
Use to indicate fill value for fillna() call

Example call:
```bash
$ luigi --module final_project.tasks DataCleaner --local-scheduler --date-data --date-column Date --na-filler 0
```

### DataEncoder(Task)
Requires: DataCleaner  
Output: LocalTarget - saves csv file under 'data/encoded/'  
Run: Encodes categorical data as integers or binary data as dummy variables. Pass in as parameter the column to encode.

#### Luigi Parameters
###### cols
_Luigi DictParameter, default={"cat": "none", "dum": "none"}_  
Use to tell DataEncoder which column to use as categorical and which as dummy.  

Example call:
```bash
$ luigi --module final_project.tasks DataEncoder --local-scheduler --cols '{"cat": "categorical_col", "dum": "dummy_col"}'
```

### DataVisualizer(Task)
Requires: DataCleaner (or DataEncoder if encoder = True)  
Output: LocalTarget - saves pdf file under 'data/visualized/'  
Run: Creates a Seaborn chart using your data and parameter specifications.

#### Luigi Parameters
##### Data Settings
###### figure_name
_Luigi Parameter, default='figure.pdf'_  
Use to change name of output file.

###### encoder
_Luigi BoolParameter, default=False_  
Use to indicate if you need to encode any of your data.

##### Chart Options
###### func
_Luigi Parameter, default="lm"_  
Use to indicate which Seaborn function you want to use  
Options are "lm" for lmplot, "cat" for catplot, "facet" for FacetGrid, "pair" for PairGrid
###### kind (not used for func="lm")
_Luigi Parameter, default="point"_  
Use to indicate which Seaborn chart kind you want to use.  
Options for catplot: “point”, “bar”, “strip”, “swarm”, “box”, “violin”, or “boxen”  
Options for FacetGrid or PairGrid: "scatter," "point," "hist," "bar" or "dist"

##### Variables (must specify, except if using PairGrid)
###### xyvars
_Luigi DictParameter, default={"x": "x", "y": "y"}_  
Use to specify x and y variables to chart, and facet variable ("facet": "<facet-variable>") when using FacetGrid  

Example Calls:  
* catplot with kind "strip"
```bash
$ luigi --module final_project.tasks DataVisualizer --local-scheduler --func cat --kind strip --xyvars '{"x": "x_variable", "y":"y_variable"}'
```

* lmplot with encoding
```bash
$ luigi --module final_project.tasks DataVisualizer --local-scheduler --func lm --xyvars '{"x": "x_variable", "y":"y_variable"}' --encoder --DataEncoder-cols '{"cat": "categorical_col"}'
```

* FacetGrid with kind "scatter"
```bash
$ luigi --module final_project.tasks DataVisualizer --local-scheduler --func facet --kind scatter --xyvars '{"x": "x_variable", "y":"y_variable", "facet":"facet_variable"}'
```  

***  

## Parameter Table
__GetBadData__

Parameter | Default | Content | Use
----------|---------|---------|----
filename  | 'clean_file.csv' | name of file | rename output file
  
__DataCleaner__  

Parameter | Default | Content | Use
----------|---------|---------|----
date_data | False | boolean | are we parsing dates?
date_column | "date" | name of date column | which column to date-parse
drop_nan | "none" | none," "row," "column," or "both" | where to drop data with missing values
na_filler| ' ' | what to fill NaN's with | fillna() values

__DataEncoder__  

Parameter | Content | Use
----------|---------|----
cols | dict {"cat": <categorical-column>, "dum": <dummy-column>} | indicate columns to encode as categorical or dummy variables

__DataVisualizer__  

Parameter | Default | Content | Use
----------|---------|---------|----
figure_name | 'figure.pdf' | name of figure | rename output figure
encoder | False | boolean | are we encoding data?
func | "lm" | lm," "cat," "facet," or "pair" | indicate use of Seaborn's lmplot, catplot, FacetGrid or PairGrid
kind | "point" | catplot: “point”, “bar”, “strip”, “swarm”, “box”, “violin”, or “boxen.” FacetGrid or PairGrid: "scatter," "point," "hist," "bar" or "dist." | indicate which "kind" of Seaborn plot
xyvars | {"x": "x", "y": "y"} | Dict {"x": <x>, "y": <y>, "facet": <facet>} | specify x and y variables to plot (and facet variable for FacetGrid)
