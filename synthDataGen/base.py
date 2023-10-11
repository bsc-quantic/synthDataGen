import os
import json
import re

from typing import Dict, List, Tuple, Set

import pandas as pd
import numpy as np
from scipy.stats import truncnorm
from datetime import datetime, timedelta


class Controller:

    def __init__(self):
        return None
    
    @property
    def include29February(self):
        return self._include29February

    @property
    def dataSourceOptions(self):
        return self._dataSourceOptions

    @property
    def dataSource(self):
        return self._dataSource

    @property
    def inputJSON(self):
        return self._inputJSON

    @include29February.setter
    def include29February(self, new_include29February: str):
        self._include29February = new_include29February

    @dataSourceOptions.setter
    def dataSourceOptions(self, new_dataSourceOptions: str):
        self._dataSourceOptions = new_dataSourceOptions

    @dataSource.setter
    def dataSource(self, new_dataSource: str):
        self._dataSource = new_dataSource
    
    @inputJSON.setter
    def inputJSON(self, new_inputJSON: str):
        self._inputJSON = new_inputJSON

    def _parseMainData(self, data):
        # Basic parameters
        self._dataSourceOptions: List[str] = data["dataSourceOptions"]
        self._dataSource: str = data["dataSource"]

    def loadMainParams(self, paramsFileName: str):
        """Loads the main parameters from the specified input JSON.
        May be useful to reload the input parameters file at any time during the execution. The corresponding controller instances should be created again, passing to them the inputJSON member (which has been reloaded).

        :param str paramsFileName: the name of the JSON file containing the dictionary (absolute or relative path)
        """

        if not os.path.isfile(paramsFileName):
            raise Exception("File '" + paramsFileName + "' does not exist.")

        with open(paramsFileName, 'r') as jsonFile:
            self._inputJSON = json.load(jsonFile)

            self._parseMainData(self._inputJSON)

            if self.dataSource not in self.dataSourceOptions:
                raise ValueError("UNKNOWN DATA SOURCE '" + self.dataSource + "'")
            elif self.dataSource == "ESIOS":
                self._dataInstance = self.ESIOSController()
                self._dataInstance.loadData(self._inputJSON)
            elif self.dataSource == "localDF":
                self._dataInstance = self.LocalDFController()
                self._dataInstance.loadData(self._inputJSON)

    def __filter29February(self, df: pd.DataFrame, include29February: bool) -> pd.DataFrame:
        if not include29February:
            if self._dataSource == "localDF":
                df = df.rename({str(entry):"2024" + "-" + str(entry) for entry in df.index})      # This mental retardation is for pandas to not check the February 29th when parsing the index to datetime
                df.index = pd.to_datetime(df.index)

            df = df[~((df.index.month == 2) & (df.index.day == 29))]

            if len(set(df.index.year)) == 1:
                df.index = df.index + pd.offsets.DateOffset(years = 2023 - df.index.year[1])

            return df

    def getDataFromSource(self, initialYear: int = None, initDatetime: datetime = datetime.now(), hoursAhead: int = None, include29February = False) -> pd.DataFrame:
        """Get the data from source considering the specified parameters. 
        If some parameter is not provided, the one from the input file is used by default.

        :param int initialYear: first year considered for the request.
        :param datetime initDatetime: the initial (MM-DD-hh-mm) considered for the request.
        :param int hoursAhead: hours from 'initDatetime' on that we want to consider for the request.
        :param bool include29February: indicates whether or not to include the February 29 in the returned DataFrame.
        :returns pandas.DataFrame:
        """

        df: pd.DataFrame = self._dataInstance.getDataFromSource(initialYear, initDatetime, hoursAhead)

        df = self.__filter29February(df, include29February)

        return df

    class ESIOSController():

        from synthDataGen.common import bibliotecaEsios

        def __init__(self):
            return None

        def loadData(self, data: Dict):
            self._keysFileDir: str = data["ESIOS_params"]["keysFileDir"]
            self._keysFileName: str = data["ESIOS_params"]["keysFileName"]

            keysFile = os.path.join(self.keysFileDir, self.keysFileName)
            with open(keysFile, 'r') as keysJSONFile:
                esiosKeyData = json.load(keysJSONFile)
                self.__esiosKey = esiosKeyData["ESIOS_KEY"]    # We'll let this private

            self._indicador: List[int] = data["ESIOS_params"]["indicador"]
            self._time_trunc: str = data["ESIOS_params"]["time_trunc"]

        @property
        def keysFileDir(self):
            return self._keysFileDir
        
        @property
        def keysFileName(self):
            return self._keysFileName
        
        @property
        def indicador(self):
            return self._indicador

        @property
        def time_trunc(self):
            return self._time_trunc

        @keysFileDir.setter
        def keysFileDir(self, new_keysFileDir: str):
            self._keysFileDir = new_keysFileDir

        @keysFileName.setter
        def keysFileName(self, new_keysFileName: str):
            self._keysFileName = new_keysFileName

        @indicador.setter
        def indicador(self, new_indicador: str):
            self._indicador = new_indicador

        @time_trunc.setter
        def time_trunc(self, new_time_trunc: str):
            self._time_trunc = new_time_trunc

        def _getDataForYear(self, year: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
            initialDate: datetime = datetime(year, 
                                            initDatetime.month, initDatetime.day, initDatetime.hour, initDatetime.minute)
            endDate: datetime = initialDate + timedelta(hours = hoursAhead)

            return esios.dataframe_lista_de_indicadores_de_esios_por_fechas([self.indicador], 
                                                                            initialDate, endDate,
                                                                            time_trunc = self.time_trunc,
                                                                            inlcuye29DeFebrero = '').filter(["value"], axis = 1)

        def _getDataForFirstYear(self, initialYear: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
            return self._getDataForYear(initialYear, initDatetime, hoursAhead, esios).rename(columns = {"value": initialYear})

        def _getDataForTheRestOfYears(self, df: pd.DataFrame, initialYear: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
            for year in range(initialYear + 1, datetime.now().year):
                df[year] = list(self._getDataForYear(year, initDatetime, hoursAhead, esios)["value"])

            return df

        def getDataFromSource(self, initialYear: int = None, initDatetime: datetime = datetime.now(), hoursAhead: int = None) -> pd.DataFrame:
            esiosInstance = self.bibliotecaEsios.BajadaDatosESIOS(self.__esiosKey)

            df: pd.DataFrame = self._getDataForFirstYear(initialYear, initDatetime, hoursAhead, esiosInstance)
            df = self._getDataForTheRestOfYears(df, initialYear, initDatetime, hoursAhead, esiosInstance)

            # Shift the index (row names) to the current date
            df.index = df.index + pd.offsets.DateOffset(years = initDatetime.year - initialYear)

            return df
    
    class LocalDFController():

        def __init__(self):
            return None

        def loadData(self, data: Dict):
            self._dataFrameDir: str = data["localDF_params"]["dataFrameDir"]
            self._dataframeFileName: str = data["localDF_params"]["dataframeFileName"]

            self._dataFrameFile: str = os.path.join(self.dataFrameDir, self.dataframeFileName)

            self._columnToAnalyze: str = data["localDF_params"]["columnToAnalyze"]
            self._skipFirstColumn: bool = data["localDF_params"]["skipFirstColumn"]

            self._datetimeColumnName: str = data["localDF_params"]["datetimeColumnName"]
            self._datetimeFormat: str = data["localDF_params"]["datetimeFormat"]

        @property
        def dataFrameDir(self):
            return self._dataFrameDir

        @property
        def dataframeFileName(self):
            return self._dataframeFileName

        @property
        def dataFrameFile(self):
            return self._dataFrameFile
        
        @property
        def skipFirstColumn(self):
            return self._skipFirstColumn
        
        @property
        def columnToAnalyze(self):
            return self._columnToAnalyze

        @property
        def datetimeColumnName(self):
            return self._datetimeColumnName
        
        @property
        def datetimeFormat(self):
            return self._datetimeFormat 

        @dataFrameDir.setter
        def dataFrameDir(self, new_dataFrameDir: str):
            self._dataFrameDir = new_dataFrameDir

        @dataframeFileName.setter
        def dataframeFileName(self, new_dataframeFileName: str):
            self._dataframeFileName = new_dataframeFileName
        
        @dataFrameFile.setter
        def dataFrameFile(self, new_dataFrameFile: str):
            self._dataFrameFile = new_dataFrameFile

        @skipFirstColumn.setter
        def skipFirstColumn(self, new_skipFirstColumn: str):
            self._skipFirstColumn = new_skipFirstColumn

        @columnToAnalyze.setter
        def columnToAnalyze(self, new_columnToAnalyze: str):
            self._columnToAnalyze = new_columnToAnalyze
        
        @datetimeColumnName.setter
        def datetimeColumnName(self, new_datetimeColumnName: str):
            self._datetimeColumnName = new_datetimeColumnName

        @datetimeFormat.setter
        def datetimeFormat(self, new_datetimeFormat: str):
            self._datetimeFormat = new_datetimeFormat

        def __createDateColumns(self, df: pd.DataFrame):
            df[self.datetimeColumnName] = pd.to_datetime(df[self.datetimeColumnName], format = self.datetimeFormat)
            df.drop_duplicates(subset = [self.datetimeColumnName], inplace = True)

            df["dateNoYear"] = df[self.datetimeColumnName].dt.strftime("%m-%d %H:%M:%S")
            df["year"] = df[self.datetimeColumnName].dt.year

            df.set_index("dateNoYear", inplace = True)

        def __rearrangeDFByYear(self, df: pd.DataFrame) -> pd.DataFrame:
            years = list(set(df["year"]));  years.sort()

            resultDF = pd.DataFrame()
            for year in years:
                dataframe = pd.Series.to_frame(df[df["year"] == year][self.columnToAnalyze])
                dataframe = dataframe.rename(columns = {str(self.columnToAnalyze):str(year)})

                resultDF = pd.merge(resultDF, dataframe, left_index = True, right_index = True, how = "outer")

            # resultDF = resultDF.rename({str(entry):str(datetime.now().year) + "-" + str(entry) for entry in resultDF.index})

            return resultDF

        def getDataFromSource(self, initialYear: int = None, initDatetime: datetime = datetime.now(), hoursAhead: int = None) -> pd.DataFrame:
            df = pd.read_csv(self.dataFrameFile)

            if self.skipFirstColumn:
                df = df.iloc[:, 1:]

            self.__createDateColumns(df)
            return self.__rearrangeDFByYear(df)


class Adjustments():

    from synthDataGen.common import bibliotecaGeneral

    def __init__(self, dataDict: Dict):
        return None

    def _extractYearFromStr(self, literal: str | int) -> int:
        reSult = re.match("^[a-zA-Z]*(\d{4})$", str(literal))

        if reSult:
            return int(reSult.group(1))
        else:
            raise Exception("Unable to extract year from literal '" + literal + "'.")

    def _getListOfYears(self, df: pd.DataFrame) -> List[str]:
        columnNames: List = list(df.columns)
        
        years: List = []
        for columnName in columnNames:
            year = self._extractYearFromStr(columnName)

            if year not in years:
                years.append(year)
            
        return years

    def _checkDataFrameContiguity(self, years: List[int]):
        if not sorted(years) == list(range(min(years), max(years) + 1)):
            raise ValueError("Not valid DataFrame. Years (column indices) MUST be continguous.")

    def _checkAdjustmentsDict(self, years: List[int], adjustmentsDict: Dict):
        if not all(isinstance(element, int) for element in adjustmentsDict.keys()):
            raise ValueError("Not valid 'adjustmentsDict'. All values in it MUST be integers.")

        providedAdjustments: List[int] = []
        for year in years:
            if year in adjustmentsDict:
                providedAdjustments.append(year)

        print("Adjusting years: " + ','.join(map(str, providedAdjustments)))

    def performAnualAdjustments(self, df: pd.DataFrame, adjustmentsDict: Dict = None) -> pd.DataFrame:
        """Performs an anual adjustments on the current dataframe with the provided dictionary.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the adjustment should be applied.
        :param dict adjustmentsDict: dictionary of percentages of adjustment by year.
        :returns pandas.DataFrame:
        """

        years: List[str] = self._getListOfYears(df)

        self._checkDataFrameContiguity(years)
        self._checkAdjustmentsDict(years, adjustmentsDict)

        for element in df.columns:
            year = self._extractYearFromStr(element)

            if year in adjustmentsDict:
                df[element] = df[element] * (1 + adjustmentsDict[year] / 100)
        
        return df
    
    def _checkFrequencyFormatIsValid(self, frequency: str):
        if not re.match("\d+(\.\d+)?[DHTS]", frequency):
            raise ValueError("Frequency '" + frequency + "' not valid. It should be an integer followed by a unit ('D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes.")

    def _getFreqNormalized(self, dfIndex) -> str:
        frequency = pd.infer_freq(dfIndex)
        if not frequency:
            possibleFreqs: Set = set(np.diff(dfIndex))
            for freq in possibleFreqs:
                if freq > 0: frequency = pd.tseries.frequencies.to_offset(pd.Timedelta(freq)).freqstr

        reSult = re.match("([DHTS])", frequency)
        if reSult:
            return "1" + str(reSult.group(1))

        return frequency

    def _checkCoarserDFResolution(self, df: pd.DataFrame, frequency: str):
        dfFreq: str = self._getFreqNormalized(df.index)
        
        dfDelta: pd.Timedelta = pd.Timedelta(dfFreq)
        freqDelta: pd.Timedelta = pd.Timedelta(frequency)

        if freqDelta > dfDelta:
            raise ValueError("The provided frequency '" + frequency + "' is of a coarser resolution than the one of the DataFrame ('" + dfFreq + "'). Please, choose a finer one for the data to be upsampled.")

    def upsample(self, df: pd.DataFrame, frequency: str = None, method: str = None, **kwargs) -> pd.DataFrame:
        """Interpolates the DataFrame by rows, considering the upsampling frequency (which must be finer-grained), method and spline order for interpolation.
        It uses the pandas.DataFrame.interpolate(method, splineOrder) method.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the upsampling should be applied.
        :param int frequency: the required output frequency.
        :param int method: the method by means of which the upsampling will be performed. For 'polynomial' and 'spline' an 'order' must be specified in \*\*kwargs.
        :param *optional* ``kwargs``: keyword arguments to pass on to the interpolation function.
        :returns pandas.DataFrame:
        """

        self._checkFrequencyFormatIsValid(frequency)
        self._checkCoarserDFResolution(df, frequency)

        polynomialMethods: List[str] = ["polynomial", "spline"]
        acceptedInterpolationMethods: List[str] = [*polynomialMethods]

        if method in polynomialMethods:
            if "order" in kwargs:
                order = kwargs["order"]

            return self.bibliotecaGeneral.resampleaDataFrame(df, frequency, method, order)
        else:
            raise ValueError("Interpolation method '" + method + "' not implemented. Please choose some: " + ', '.join(acceptedInterpolationMethods) + ".")
        
    def _checkFinerDFResolution(self, df: pd.DataFrame, frequency: str):
        dfFreq: str = self._getFreqNormalized(df.index)
        
        dfDelta: pd.Timedelta = pd.Timedelta(dfFreq)
        freqDelta: pd.Timedelta = pd.Timedelta(frequency)

        if freqDelta < dfDelta:
            raise ValueError("The provided frequency '" + frequency + "' is of a finer resolution than the one of the DataFrame ('" + dfFreq + "'). Please, choose a coarser one for the data to be aggregated into.")

    def downsample(self, df: pd.DataFrame, frequency: str = None, aggregationFunc = None) -> pd.DataFrame:
        """Aggregates the DataFrame by means of an aggregation function, getting a new DataFrame with the specified frequency (which must be coarser-grained).
        It uses the pandas.Dataframe.resample(rule) & the pandas.core.resample.Resampler.aggregate(func) methods.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the downsampling should be applied.
        :param str frequency: the resulting frequency under which the DataFrame should be aggregated.
        :param function | str aggregationFunc: a function (e.g. lambda x: x.mean()) or a string (e.g. "mean") representing the aggregation function to be applied.
        :returns pandas.DataFrame:
        """

        self._checkFrequencyFormatIsValid(frequency)
        self._checkFinerDFResolution(df, frequency)

        return df.resample(frequency).agg(aggregationFunc)
    
class Sampling:

    _availProbDistibutions: List = ["'truncnorm'"]

    def __init__(self, dataDict: Dict):
        return None

    def _getMeanAndStdForAxis(self, df: pd.DataFrame, axis: int) -> Tuple[List, List]:
        means: pd.Series = df.mean(axis = axis)
        stds: pd.Series = df.std(axis = axis, ddof = 0)

        return (means, stds)
        
    def _getSamples_truncnorm(self, df: pd.DataFrame, numberOfSamples: int, means: List[float], stds: List[float]) -> pd.DataFrame:
        resultingDataFrame: pd.DataFrame = pd.DataFrame()

        for index, mu, sigma in zip(df.index, means, stds):
        # for index, (mu, sigma) in enumerate(zip(means, stds)):
            lowerBound = 0
            upperBound = mu + 2*sigma

            pdf = truncnorm((lowerBound - mu) / sigma,
                            (upperBound - mu) / sigma,
                            loc = mu, scale = sigma)

            samples = pdf.rvs(numberOfSamples)
            resultingDataFrame[index] = samples
        
        return resultingDataFrame

    def getSamples(self, df: pd.DataFrame, numberOfSamples: int = None, probDistribution: str = None) -> pd.DataFrame:
        """Gets a number of samples for every column in the provided DataFrame. A truncated normal probability distribution is used to do so.

        :param pandas.DataFrame df: the input DataFrame to be considered.
        :param int numberOfSamples: the number of samples that will be returned (number of rows).
        :param str probDistribution: a string defining the probability distribution to be used. For instance "truncnorm".
        :returns pandas.DataFrame:
        """

        if not numberOfSamples: numberOfSamples = self.numberOfSamples
        if not probDistribution: probDistribution = self.probDistribution

        if probDistribution == "truncnorm":
            means, stds = self._getMeanAndStdForAxis(df, 1)
            return self._getSamples_truncnorm(df, numberOfSamples, means, stds)
        else:
            raise ValueError("Probability distribution '" + probDistribution +"' not available for sampling. Please choose one of the following: " + ', '.join(self._availProbDistibutions))