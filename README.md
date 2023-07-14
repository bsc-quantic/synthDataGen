# synthDataGen

## Usage

The controller class is designed to be used in 2 different ways:
1. By means of the input parameters file ("inputParams.json")
2. By passing the required parameters to each method every time

While the first approach is the more straighforward one, where the user just applies the methods one after the other as they were designed, the second approach is more adaptable and is tailored to admit arbitrary parameters in each step, in case the user modifies the DataFrame in between method calls.

## Examples

## Input parameters file approach

The following examples have been included and extended in the fullExample.ipynb in ./notebooks.

```
from synthDataGen.controller import DataControllerESIOS

controller = DataControllerESIOS("inputParams.json", "./synthDataGen/settings/")

df = controller.getDataFromSource()
df = controller.performAnualAdjustments(df)
df = controller.resampleOnAxis0(df)
```

## Arguments passing

```
from synthDataGen.controller import DataControllerESIOS

controller = DataControllerESIOS("inputParams.json", "./synthDataGen/settings/")

df = controller.getDataFromSource(initialYear=2018, initDatetime=d, hoursAhead=6)
df = controller.performAnualAdjustments(df, adjustmentsDict={2018: 1.012, 2019: 1.023, 2020: 1.0145, 2021: 1.03, 2022: 1.08})
df = controller.resampleOnAxis0(df, sampleFreqInMins=20, method="spline", order=3)
```