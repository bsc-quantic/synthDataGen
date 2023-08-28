import os
import json
import re

from typing import Dict, List, Tuple

import pandas as pd
from scipy.stats import truncnorm
from datetime import datetime, timedelta


class Controller:

    def __init__(self):
        return None

    @property
    def initialYear(self):
        return self._initialYear
    
    @property
    def hoursAhead(self):
        return self._hoursAhead
    
    @property
    def dataSourceOptions(self):
        return self._dataSourceOptions

    @property
    def dataSource(self):
        return self._dataSource

    @property
    def inputJSON(self):
        return self._inputJSON

    @initialYear.setter
    def initialYear(self, new_initialYear: str):
        self._initialYear = new_initialYear

    @hoursAhead.setter
    def hoursAhead(self, new_hoursAhead: str):
        self._hoursAhead = new_hoursAhead

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
        self._initialYear: int = data["loadDataParams"]["sourceFilters"]["initialYear"] 
        self._hoursAhead: int = data["loadDataParams"]["sourceFilters"]["hoursAhead"]
        
        self._dataSourceOptions: List[str] = data["loadDataParams"]["dataSourceOptions"]
        self._dataSource: str = data["loadDataParams"]["dataSource"]

    def loadMainParams(self, paramsFileName: str, directory: str = os.getcwd()):
        """Loads the main parameters from the specified input JSON.
        May be useful to reload the input parameters file at any time during the execution. The corresponding controller instances should be created again, passing to them the inputJSON member (which has been reloaded).

        :param str paramsFileName: the name of the JSON file containing the dictionary.
        :param str directory: the directory in which the paramsFileName is located.
        """

        self._paramsFileName = paramsFileName
        self._paramsFileDirectory = directory

        self._inputParamsFile = os.path.join(directory, paramsFileName)
        with open(self._inputParamsFile, 'r') as jsonFile:
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
                

    def getDataFromSource(self, initialYear: int = None, initDatetime: datetime = datetime.now(), hoursAhead: int = None) -> pd.DataFrame:
        """Get the data from source considering the specified parameters. 
        If some parameter is not provided, the one from the input file is used by default.

        :param int initialYear: first year considered for the request.
        :param datetime initDatetime: the initial (MM-DD-hh-mm) considered for the request.
        :param int hoursAhead: hours from 'initDatetime' on that we want to consider for the request.
        :returns pandas.DataFrame:
        """

        if not initialYear: initialYear = self.initialYear
        if not hoursAhead: hoursAhead = self.hoursAhead

        return self._dataInstance.getDataFromSource(initialYear, initDatetime, hoursAhead)

    class ESIOSController():

        from synthDataGen.common import bibliotecaEsios

        def __init__(self):
            return None

        def loadData(self, data: Dict):
            self._keysFileDir: str = data["loadDataParams"]["ESIOS_params"]["keysFileDir"]
            self._keysFileName: str = data["loadDataParams"]["ESIOS_params"]["keysFileName"]

            keysFile = os.path.join(self.keysFileDir, self.keysFileName)
            with open(keysFile, 'r') as keysJSONFile:
                esiosKeyData = json.load(keysJSONFile)
                self.__esiosKey = esiosKeyData["ESIOS_KEY"]    # We'll let this private

            self._indicadores: List[int] = data["loadDataParams"]["ESIOS_params"]["indicadores"]
            self._time_trunc: str = data["loadDataParams"]["ESIOS_params"]["time_trunc"]

        @property
        def keysFileDir(self):
            return self._keysFileDir
        
        @property
        def keysFileName(self):
            return self._keysFileName
        
        @property
        def indicadores(self):
            return self._indicadores

        @property
        def time_trunc(self):
            return self._time_trunc

        @keysFileDir.setter
        def keysFileDir(self, new_keysFileDir: str):
            self._keysFileDir = new_keysFileDir

        @keysFileName.setter
        def keysFileName(self, new_keysFileName: str):
            self._keysFileName = new_keysFileName

        @indicadores.setter
        def indicadores(self, new_indicadores: str):
            self._indicadores = new_indicadores

        @time_trunc.setter
        def time_trunc(self, new_time_trunc: str):
            self._time_trunc = new_time_trunc

        def _getDataForYear(self, year: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
            initialDate: datetime = datetime(year, 
                                            initDatetime.month, initDatetime.day, initDatetime.hour, initDatetime.minute)
            endDate: datetime = initialDate + timedelta(hours = hoursAhead)

            return esios.dataframe_lista_de_indicadores_de_esios_por_fechas(self.indicadores, 
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
            if not initialYear: initialYear = self.initialYear
            if not hoursAhead: hoursAhead = self.hoursAhead

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
            self._dataFrameDir: str = data["loadDataParams"]["localDF_params"]["dataFrameDir"]
            self._dataframeFileName: str = data["loadDataParams"]["localDF_params"]["dataframeFileName"]

            self._dataFrameFile: str = os.path.join(self.dataFrameDir, self.dataframeFileName)

            self._indexColumnName: str = data["loadDataParams"]["localDF_params"]["indexColumnName"]

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
        def indexColumnName(self):
            return self._indexColumnName

        @dataFrameDir.setter
        def dataFrameDir(self, new_dataFrameDir: str):
            self._dataFrameDir = new_dataFrameDir

        @dataframeFileName.setter
        def dataframeFileName(self, new_dataframeFileName: str):
            self._dataframeFileName = new_dataframeFileName
        
        @dataFrameFile.setter
        def dataFrameFile(self, new_dataFrameFile: str):
            self._dataFrameFile = new_dataFrameFile
        
        @indexColumnName.setter
        def indexColumnName(self, new_indexColumnName: str):
            self._indexColumnName = new_indexColumnName


class Adjustments():

    from synthDataGen.common import bibliotecaGeneral

    def __init__(self, dataDict: Dict):
        # Adjustment parameters
        self._adjustmentByYear: Dict[int, int] = {int(key): value for key, value in dataDict["adjustmentByYear"].items()}

        # Up/down sampling parameters
        self._upsampling_frequency: str = dataDict["upsampling_params"]["frequency"]
        self._upsampling_method: str = dataDict["upsampling_params"]["method"]
        self._upsampling_splineOrder: int = dataDict["upsampling_params"]["splineOrder"]

        self._downsampling_frequency: str = dataDict["downsampling_params"]["frequency"]
        self._downsampling_aggregationFunc: str = dataDict["downsampling_params"]["aggregationFunc"]

    @property
    def adjustmentByYear(self):
        return self._adjustmentByYear

    @property
    def upsampling_frequency(self):
        return self._upsampling_frequency
    
    @property
    def upsampling_method(self):
        return self._upsampling_method
    
    @property
    def upsampling_splineOrder(self):
        return self._upsampling_splineOrder
    
    @property
    def downsampling_frequency(self):
        return self._downsampling_frequency
    
    @property
    def downsampling_aggregationFunc(self):
        return self._downsampling_aggregationFunc

    @adjustmentByYear.setter
    def adjustmentByYear(self, new_adjustmentByYear: str):
        self._adjustmentByYear = new_adjustmentByYear

    @upsampling_frequency.setter
    def upsampling_frequency(self, new_upsampling_frequency: str):
        self._upsampling_frequency = new_upsampling_frequency
    
    @upsampling_method.setter
    def upsampling_method(self, new_upsampling_method: str):
        self._upsampling_method = new_upsampling_method

    @upsampling_splineOrder.setter
    def upsampling_splineOrder(self, new_upsampling_splineOrder: str):
        self._upsampling_splineOrder = new_upsampling_splineOrder

    @downsampling_frequency.setter
    def downsampling_frequency(self, new_downsampling_frequency: str):
        self._downsampling_frequency = new_downsampling_frequency

    @downsampling_aggregationFunc.setter
    def downsampling_aggregationFunc(self, new_downsampling_aggregationFunc: str):
        self._downsampling_aggregationFunc = new_downsampling_aggregationFunc

    def _checkDataFrameContiguity(self, df: pd.DataFrame):
        years: List = list(df.columns)

        if not sorted(years) == list(range(min(years), max(years) + 1)):
            raise ValueError("Not valid DataFrame. Years (column indices) MUST be continguous.")

    def _checkAdjustmentsDict(self, initialYear: int, adjustmentsDict: Dict):
        if not all(isinstance(element, int) for element in adjustmentsDict.keys()):
            raise ValueError("Not valid 'adjustmentsDict'. All values in it MUST be integers.")

        years_adjustmentsDict: List = sorted([year for year in adjustmentsDict.keys()])
        years_fromInitialYear: List = sorted([year for year in range(initialYear, datetime.now().year)])

        if years_adjustmentsDict != years_fromInitialYear:
            raise ValueError("Not valid adjustments dictionary. It MUST contain an entry for every year since the first year in the provided DataFrame.")

    def performAnualAdjustments(self, df: pd.DataFrame, adjustmentsDict: Dict = None) -> pd.DataFrame:
        """Performs an anual adjustments on the current dataframe with the provided dictionary.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the adjustment should be applied.
        :param dict adjustmentsDict: dictionary of percentages of adjustment by year.
        :returns pandas.DataFrame:
        """

        if not adjustmentsDict: adjustmentsDict = self.adjustmentByYear

        self._checkDataFrameContiguity(df)
        self._checkAdjustmentsDict(min(df.columns), adjustmentsDict)

        for element in df.columns:
            df[element] = df[element] * (1 + adjustmentsDict[element] / 100)
        
        return df
    
    def _checkFrequencyFormatIsValid(self, frequency: str):
        if not re.match("\d+(\.\d+)?[DHTS]", frequency):
            raise ValueError("Frequency '" + frequency + "' not valid. It should be an integer followed by a unit ('D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes.")

    def _getFreqNormalized(self, frequency: str) -> str:
        reSult = re.match("([DHTS])", frequency)
        if reSult:
            return "1" + str(reSult.group(1))

        return frequency

    def _checkCoarserDFResolution(self, df: pd.DataFrame, frequency: str):
        dfFreq: str = self._getFreqNormalized(pd.infer_freq(df.index))
        
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

        if not frequency: frequency = self.upsampling_frequency
        if not method: method = self.upsampling_method

        self._checkFrequencyFormatIsValid(frequency)
        self._checkCoarserDFResolution(df, frequency)

        polynomialMethods: List[str] = ["polynomial", "spline"]
        acceptedInterpolationMethods: List[str] = [*polynomialMethods]

        if method in polynomialMethods:
            order: int = self.upsampling_splineOrder
            if "order" in kwargs:
                order = kwargs["order"]

            return self.bibliotecaGeneral.resampleaDataFrame(df, frequency, method, order)
        else:
            raise ValueError("Interpolation method '" + method + "' not implemented. Please choose some: " + ', '.join(acceptedInterpolationMethods) + ".")
        
    def _checkFinerDFResolution(self, df: pd.DataFrame, frequency: str):
        dfFreq: str = self._getFreqNormalized(pd.infer_freq(df.index))
        
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

        if not frequency: frequency = self.downsampling_frequency
        if not aggregationFunc: aggregationFunc = self.downsampling_aggregationFunc

        self._checkFrequencyFormatIsValid(frequency)
        self._checkFinerDFResolution(df, frequency)

        if not aggregationFunc: aggregationFunc = self.downsampling_aggreationFunc
        if not frequency: frequency = self.downsampling_frequency

        return df.resample(frequency).agg(aggregationFunc)
    
class Sampling:

    _availProbDistibutions: List = ["'truncnorm'"]

    def __init__(self, dataDict: Dict):
        self._numberOfSamples: int = dataDict["sampling"]["numberOfSamples"]
        self._probDistribution: str = dataDict["sampling"]["probDistribution"]
    
    @property
    def numberOfSamples(self):
        return self._numberOfSamples

    @property
    def probDistribution(self):
        return self._probDistribution

    @numberOfSamples.setter
    def numberOfSamples(self, new_numberOfSamples: str):
        self._numberOfSamples = new_numberOfSamples

    @probDistribution.setter
    def probDistribution(self, new_probDistribution: str):
        self._probDistribution = new_probDistribution

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