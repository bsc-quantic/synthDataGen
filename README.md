# synthDataGen

## Usage

The controller class is designed to be used in 2 different ways:
1. By means of the input parameters file ("inputParams.json")
2. By passing the required parameters to each method every time

While the first approach is the more straighforward one, where the user just applies the methods one after the other as they were designed, the second approach is more adaptable and is tailored to admit arbitrary parameters in each step, in case the user modifies the DataFrame in between method calls.

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