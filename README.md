# SynthDataGen

## Usage

This app is designed to be use in the following way:

1. Load the data either from ESIOS or from a local CSV file which contains a pandas.DataFrame-like table.
2. Apply the needed adjustments by 
    - Applying a multiplication factor by year to all the rows
    - Upsampling or downsampling the data by means of different techniques
3. Get samples from the DataFrame, following the available probability distributions, for each row.

## Workflow and usage

1. Depending on the used loader (ESIOSLoader, LocalDFLoader, etc.), the corresponding attributes are expected to be specified in the corresponding nested dictionary in the **input parameters file**:
    - "ESIOS": the fields for the **access token**, the particular **indicator** and the **granularity** for the data to be requested.
    - "LocalDF": the **directory and name of the CSV file** containing the DataFrame to be loaded, the **variable name** to get (indicator), whether to **skip the first column** or not, and the **datetime format of the index column**.

2. The Loader.getDataFromSource(...) method receive a number of parameters, which are used as filters against the loaded DataFrame. So, the resulting DataFrame will start from an 
    - **initial year**, and consider 
    - from an **initial datetime** (default value: 'now') 
    - a number of **hours ahead**. 
    - Besides, whether to **discard the February 29** or not should also be specified.

3. The **adjustments by year** method receives a dictionary <year,adjustmentValue> = <int,int|float>. It is used for inflation or similar adjustments of a DataFrame. It is specified in percentage, so a 10 indicate a positive adjustment of a 10%, a -32.0 represents a negative adjustment of 32%, and a 347.89 represents just that.

4. In case a posterior **upsampling or downsampling** of the data wanted to be performed, the corresponding methods are used to specify the **granularity** when running the Adjustments.upsample(...) and Adjustments.downsample(...) methods.
    - The granularity (here **frequency**) should be an integer followed by a unit ('D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes. 
    - The interpolation **method** and the **aggregation function** for upsampling and downsampling respectively, should be specified too.

5. Finally, for **sampling** the current data by means of te Sampling.getSamples(...) method, we should provide
    - a **number of desired samples** to be generated 
    - and the **probability distribution** to consider.

## Examples

A similar example has been included and extended in the ./notebooks/fullExample.ipynb Jupyter notebook.

```
import synthDataGen.controller as controller
from datetime import datetime

controller = controller.LocalDFLoader("./synthDataGen/settings/inputParams.json")
df = controller.getDataFromSource(initialYear=2007, datetime.now(), hoursAhead=10, include29February=False)

# DataFrame adjustments
from synthDataGen.adjustments import FactorByYear, ChangeResolution

df = FactorByYear.run(df, adjustmentsDict={2022: 10, 2021: 10, 2020: 10, 2019: 10, 2018: 10, 2017: 10, 2016: 10, 2015: 10, 2014: 10, 2013: 10, 2012: 10, 2011: 10, 2010: 10, 2009: 10, 2008: 10, 2007: 10})

# Up/down sampling
df = ChangeResolution.upsample(df, frequency="15T", method="polynomial", order=2)
df = ChangeResolution.downsample(df, frequency="2H", aggregationFunc="mean")

# Samples generation
from synthDataGen.utils import Sampling

df = Sampling.getSamples(df, 5000, "truncnorm")
```

## Acknowledgements

© Copyright 2023, Germán Navarro $^\dagger$, Santiago Fernández Prieto $^{\ddagger,1}$, Ricardo Enríquez Miranda $^\ddagger$, Javier Hernanz Zájara $^\ddagger$

$^\dagger$ Barcelona Supercomputing Center
\
$^\ddagger$ Repsol Quantum Advisory Team

$^1$ Repsol-BSC Research Center

Developed within the framework of the [project CUCO](https://www.cuco.tech/). Financed by the [CDTI](https://www.cdti.es/en) and with the support of the [Spanish Ministry of Science and Innovation](https://www.ciencia.gob.es/en/) under the [Recovery, Transformation and Resilience Plan (RTRP)](https://www.ciencia.gob.es/en/Estrategias-y-Planes/Plan-de-Recuperacion-Transformacion-y-Resiliencia-PRTR.html).