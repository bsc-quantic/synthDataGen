import re
from typing import Dict, List, Set

import pandas as pd
import numpy as np

class FactorByYear():

    @staticmethod
    def _extractYearFromStr(literal) -> int:
        reSult = re.match("^[a-zA-Z]*(\d{4})$", str(literal))

        if reSult:
            return int(reSult.group(1))
        else:
            raise Exception("Unable to extract year from literal '" + literal + "'.")

    @staticmethod
    def _getListOfYears(df: pd.DataFrame) -> List[str]:
        columnNames: List = list(df.columns)
        
        years: List = []
        for columnName in columnNames:
            year = FactorByYear._extractYearFromStr(columnName)

            if year not in years:
                years.append(year)
            
        return years

    @staticmethod
    def _checkDataFrameContiguity(years: List[int]):
        if not sorted(years) == list(range(min(years), max(years) + 1)):
            raise ValueError("Not valid DataFrame. Years (column indices) MUST be continguous.")

    @staticmethod
    def _checkAdjustmentsDict(years: List[int], adjustmentsDict: Dict):
        if not all(isinstance(element, int) for element in adjustmentsDict.keys()):
            raise ValueError("Not valid 'adjustmentsDict'. All values in it MUST be integers.")

        providedAdjustments: List[int] = []
        for year in years:
            if year in adjustmentsDict:
                providedAdjustments.append(year)

        print("Adjusting years: " + ','.join(map(str, providedAdjustments)))

    @staticmethod
    def run(df: pd.DataFrame, adjustmentsDict: Dict = None) -> pd.DataFrame:
        """Performs an anual adjustments on the current dataframe with the provided dictionary.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the adjustment should be applied.
        :param dict adjustmentsDict: dictionary of percentages of adjustment by year.
        :returns pandas.DataFrame:
        """

        years: List[str] = FactorByYear._getListOfYears(df)

        FactorByYear._checkDataFrameContiguity(years)
        FactorByYear._checkAdjustmentsDict(years, adjustmentsDict)

        for element in df.columns:
            year = FactorByYear._extractYearFromStr(element)

            if year in adjustmentsDict:
                df[element] = df[element] * (1 + adjustmentsDict[year] / 100)
        
        return df
    
class ChangeResolution():

    from synthDataGen.common import bibliotecaGeneral
    
    @staticmethod
    def _checkFrequencyFormatIsValid(frequency: str):
        if not re.match("\d+(\.\d+)?[DHTS]", frequency):
            raise ValueError("Frequency '" + frequency + "' not valid. It should be an integer followed by a unit ('D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes.")
        
    @staticmethod
    def _getFreqNormalized(dfIndex) -> str:
        frequency = pd.infer_freq(dfIndex)
        if not frequency:
            possibleFreqs: Set = set(np.diff(dfIndex))
            for freq in possibleFreqs:
                if freq > 0: frequency = pd.tseries.frequencies.to_offset(pd.Timedelta(freq)).freqstr

        reSult = re.match("([DHTS])", frequency)
        if reSult:
            return "1" + str(reSult.group(1))

        return frequency
    @staticmethod
    def _checkCoarserDFResolution(df: pd.DataFrame, frequency: str):
        dfFreq: str = ChangeResolution._getFreqNormalized(df.index)
        
        dfDelta: pd.Timedelta = pd.Timedelta(dfFreq)
        freqDelta: pd.Timedelta = pd.Timedelta(frequency)

        if freqDelta > dfDelta:
            raise ValueError("The provided frequency '" + frequency + "' is of a coarser resolution than the one of the DataFrame ('" + dfFreq + "'). Please, choose a finer one for the data to be upsampled.")

    @staticmethod
    def upsample(df: pd.DataFrame, frequency: str = None, method: str = None, **kwargs) -> pd.DataFrame:
        """Interpolates the DataFrame by rows, considering the upsampling frequency (which must be finer-grained), method and spline order for interpolation.
        It uses the pandas.DataFrame.interpolate(method, splineOrder) method.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the upsampling should be applied.
        :param int frequency: the required output frequency.
        :param int method: the method by means of which the upsampling will be performed. For 'polynomial' and 'spline' an 'order' must be specified in \*\*kwargs.
        :param *optional* ``kwargs``: keyword arguments to pass on to the interpolation function.
        :returns pandas.DataFrame:
        """
        
        ChangeResolution._checkFrequencyFormatIsValid(frequency)
        ChangeResolution._checkCoarserDFResolution(df, frequency)

        polynomialMethods: List[str] = ["polynomial", "spline"]
        acceptedInterpolationMethods: List[str] = [*polynomialMethods]

        if method in polynomialMethods:
            if "order" in kwargs:
                order = kwargs["order"]

            return ChangeResolution.bibliotecaGeneral.resampleaDataFrame(df, frequency, method, order)
        else:
            raise ValueError("Interpolation method '" + method + "' not implemented. Please choose some: " + ', '.join(acceptedInterpolationMethods) + ".")
    
    @staticmethod
    def _checkFinerDFResolution(df: pd.DataFrame, frequency: str):
        dfFreq: str = ChangeResolution._getFreqNormalized(df.index)
        
        dfDelta: pd.Timedelta = pd.Timedelta(dfFreq)
        freqDelta: pd.Timedelta = pd.Timedelta(frequency)

        if freqDelta < dfDelta:
            raise ValueError("The provided frequency '" + frequency + "' is of a finer resolution than the one of the DataFrame ('" + dfFreq + "'). Please, choose a coarser one for the data to be aggregated into.")

    @staticmethod
    def downsample(df: pd.DataFrame, frequency: str = None, aggregationFunc = None) -> pd.DataFrame:
        """Aggregates the DataFrame by means of an aggregation function, getting a new DataFrame with the specified frequency (which must be coarser-grained).
        It uses the pandas.Dataframe.resample(rule) & the pandas.core.resample.Resampler.aggregate(func) methods.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the downsampling should be applied.
        :param str frequency: the resulting frequency under which the DataFrame should be aggregated.
        :param function | str aggregationFunc: a function (e.g. lambda x: x.mean()) or a string (e.g. "mean") representing the aggregation function to be applied.
        :returns pandas.DataFrame:
        """

        ChangeResolution._checkFrequencyFormatIsValid(frequency)
        ChangeResolution._checkFinerDFResolution(df, frequency)

        return df.resample(frequency).agg(aggregationFunc)