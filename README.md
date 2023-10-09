# synthDataGen

## Usage

The controller class is designed to be used in 2 different ways:
1. By means of the input parameters file ("inputParams.json")
2. By passing the required parameters to each method every time

While the first approach is the more straighforward one, where the user just applies the methods one after the other as they were designed, the second approach is more adaptable and is tailored to admit arbitrary parameters in each step, in case the user modifies the DataFrame in between method calls. See the [examples](#examples) section for a further detailed usage.

## Input parameters file explanation

The following steps are applied when the Controller.getDataFromSource() method is called:

1. Depending on the 'dataSource' field value, the corresponding attributes are expected to be specified in the corresponding nested dictionary.
    - "ESIOS": the fields for the **access token**, the particular **indicator** and the **granularity** for the data to be requested.
    - "LocalDF": the **directory and name of the CSV file** containing the DataFrame to be loaded, the **variable name** to get (indicator), whether to **skip the first column** or not, and the **datetime format of the index column**.

2. The **filters** in 'sourceFilters' are either used to get the data from the origin or the filter it afterwards. All in all, the resulting DataFrame will start from an **initial year**, and consider from the **initial datetime** parameter (default value: 'now') a number of **hours ahead**. Besides, whether to **discard the February 29** or not should also be specified.

3. The **adjustments by year** dictionary can be used for inflation adjustment or similar. It is specified over 100, so a '10' indicate a positive adjustment of a 10%.

4. In case a posterior **upsampling or downsampling** of the data wanted to be performed, the corresponding dictionaries are used to specify the **granularity** when running the Adjustments.upsample(pandas.Dataframe) and Adjustments.downsample(pandas.Dataframe) methods. The granularity should be an integer followed by a unit ('D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes. The **interpolation method** and the **aggregation function** for upsampling and downsampling respectively, should be specified too.

5. Finally, for **sampling** the current data we should provide a **number of desired samples** to be generated and the **probability distribution** to consider.

## Examples

### Input parameters file approach

The following examples have been included and extended in the fullExample.ipynb in ./notebooks.

```
from synthDataGen.base import Controller, Adjustments, Sampling

controller = Controller()
controller.loadMainParams("inputParams.json", "./synthDataGen/settings/")
df = controller.getDataFromSource()

# DataFrame adjustments
adjustments = Adjustments(controller.inputJSON)
df = adjustments.performAnualAdjustments(df)

# Up/down sampling
df = adjustments.upsample(df)
df = adjustments.downsample(df)

# Samples generation
sampling = Sampling(controller.inputJSON)
df = sampling.getSamples(df)
```

### Arguments passing

```
from synthDataGen.base import Controller, Adjustments, Sampling

from datetime import datetime

controller = Controller()
controller.loadMainParams("inputParams.json", "./synthDataGen/settings/")
df = controller.getDataFromSource(initialYear=2018, initDatetime=datetime(2023, 6, 5, 7, 0), hoursAhead=6)

# DataFrame adjustments
adjustments = Adjustments(controller.inputJSON)
df = adjustments.performAnualAdjustments(df, adjustmentsDict={2018: 1.2, 2019: 2.3, 2020: 1.45, 2021: 3, 2022: 8})

# Up/down sampling
df = adjustments.upsample(df, frequency="20T", method="spline", order=3)
df = adjustments.downsample(df, frequency="2.5H", aggregationFunc="mean")

# Samples generation
sampling = Sampling(controller.inputJSON)
df = sampling.getSamples(df, 1000, "truncnorm")
```