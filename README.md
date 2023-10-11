# synthDataGen

## Usage

The controller class is designed to be used in 2 different ways:
1. By means of the input parameters file ("inputParams.json")
2. By passing the required parameters to each method every time

While the first approach is the more straighforward one, where the user just applies the methods one after the other as they were designed, the second approach is more adaptable and is tailored to admit arbitrary parameters in each step, in case the user modifies the DataFrame in between method calls. See the [examples](#examples) section for a further detailed usage.

## Workflow and usage

1. Depending on the 'dataSource' field value, the corresponding attributes are expected to be specified in the corresponding nested dictionary in the **input parameters file**:
    - "ESIOS": the fields for the **access token**, the particular **indicator** and the **granularity** for the data to be requested.
    - "LocalDF": the **directory and name of the CSV file** containing the DataFrame to be loaded, the **variable name** to get (indicator), whether to **skip the first column** or not, and the **datetime format of the index column**.

2. The input parameters file is only used in Controller.getDataFromSource(...) method, as well as the passed parameters, which are considered as **filters** before returning the DataFrame. The resulting DataFrame will start from an 
    - **initial year**, and consider 
    - from an **initial datetime** (default value: 'now') 
    - a number of **hours ahead**. 
    - Besides, whether to **discard the February 29** or not should also be specified.

3. The **adjustments by year** method receives a dictionary <year,adjustmentValue> = <int,int|float>. It is used for inflation or similar adjustments of a DataFrame. It is specified in percentage, so a 10 indicate a positive adjustment of a 10%, a -32.0 represents a negative adjustment of 32%, and a 347.89 represents just that.

4. In case a posterior **upsampling or downsampling** of the data wanted to be performed, the corresponding methods are used to specify the **granularity** when running the Adjustments.upsample(pandas.Dataframe) and Adjustments.downsample(pandas.Dataframe) methods.
    - The granularity (here **frequency**) should be an integer followed by a unit ('D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes. 
    - The interpolation **method** and the **aggregation function** for upsampling and downsampling respectively, should be specified too.

5. Finally, for **sampling** the current data by means of te Sampling.getSamples(...) method, we should provide
    - a **number of desired samples** to be generated 
    - and the **probability distribution** to consider.

## Examples

A similar example has been included and extended in the ./notebooks/fullExample.ipynb Jupyter notebook.

```
from synthDataGen.base import Controller, Adjustments, Sampling
from datetime import datetime

controller = Controller()
controller.loadMainParams("./synthDataGen/settings/inputParams.json")
df = controller.getDataFromSource(initialYear=2007, datetime.now(), hoursAhead=10, include29February=False)

# DataFrame adjustments
adjustments = Adjustments(controller.inputJSON)
df = adjustments.performAnualAdjustments(df, adjustmentsDict={2022: 10, 2021: 10, 2020: 10, 2019: 10, 2018: 10, 2017: 10, 2016: 10, 2015: 10, 2014: 10, 2013: 10, 2012: 10, 2011: 10, 2010: 10, 2009: 10, 2008: 10, 2007: 10})

# Up/down sampling
df = adjustments.upsample(df, frequency="15T", method="polynomial", order=2)
df = adjustments.downsample(df, frequency="2H", aggregationFunc="mean")

# Samples generation
sampling = Sampling(controller.inputJSON)
df = sampling.getSamples(df, 5000, "truncnorm")
```