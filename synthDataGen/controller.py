import os
import json
import re

from typing import Dict, List

import pandas as pd
from datetime import datetime, timedelta


class DataController:

    def __init__(self):
        raise Exception("NON-INITIALIZER CLASS")

    def _parseMainData(self, data):
        # Basic parameters
        self._initialYear: int = data["initialYear"]
        self._hoursAhead: int = data["hoursAhead"]
        
        self._dataSource: str = data["dataSource"]
    
        # Adjustment parameters
        self._adjustmentByYear: Dict[int, int] = {int(key): value for key, value in data["adjustmentByYear"].items()}

        # Resampling parameters
        self._resampling_sampleFreqInMins: int = data["resampling_params"]["sampleFreqInMins"]
        self._resampling_method: str = data["resampling_params"]["method"]
        self._resampling_splineOrder: int = data["resampling_params"]["splineOrder"]

        self._downsampling_frequency: str = data["downsampling_params"]["frequency"]
        self._downsampling_aggreationFunc: str = data["downsampling_params"]["aggreationFunc"]

    @property
    def initialYear(self):
        return self._initialYear
    
    @property
    def hoursAhead(self):
        return self._hoursAhead
    
    @property
    def dataSource(self):
        return self._dataSource

    @property
    def adjustmentByYear(self):
        return self._adjustmentByYear

    @property
    def resampling_sampleFreqInMins(self):
        return self._resampling_sampleFreqInMins
    
    @property
    def resampling_method(self):
        return self._resampling_method
    
    @property
    def resampling_splineOrder(self):
        return self._resampling_splineOrder

    @initialYear.setter
    def initialYear(self, new_initialYear: str):
        self._initialYear = new_initialYear

    @hoursAhead.setter
    def hoursAhead(self, new_hoursAhead: str):
        self._hoursAhead = new_hoursAhead

    @dataSource.setter
    def dataSource(self, new_dataSource: str):
        self._dataSource = new_dataSource

    @adjustmentByYear.setter
    def adjustmentByYear(self, new_adjustmentByYear: str):
        self._adjustmentByYear = new_adjustmentByYear

    @resampling_sampleFreqInMins.setter
    def resampling_sampleFreqInMins(self, new_resampling_sampleFreqInMins: str):
        self._resampling_sampleFreqInMins = new_resampling_sampleFreqInMins
    
    @resampling_method.setter
    def resampling_method(self, new_resampling_method: str):
        self._resampling_method = new_resampling_method

    @resampling_splineOrder.setter
    def resampling_splineOrder(self, new_resampling_splineOrder: str):
        self._resampling_splineOrder = new_resampling_splineOrder


class DataControllerESIOS(DataController):

    from synthDataGen.common import bibliotecaEsios
    from synthDataGen.common import bibliotecaGeneral

    def __init__(self, paramsFileName: str, directory: str = os.getcwd()):
        self._paramsFileName = paramsFileName
        self._paramsFileDirectory = directory

        inputParamsFile = os.path.join(directory, paramsFileName)
        self._loadParams(inputParamsFile)

    def _loadParams(self, inputParamsFile: str):
        with open(inputParamsFile, 'r') as jsonFile:
            data = json.load(jsonFile)

            self._parseMainData(data)

            if self.dataSource == "ESIOS":
                self.__loadDataFromESIOS(data)
            else:
                raise ValueError("UNKNOWN DATA SOURCE " + self.dataSource)

    def __loadDataFromESIOS(self, data: Dict):
        self._keysFileDir: str = data["ESIOS_params"]["keysFileDir"]
        self._keysFileName: str = data["ESIOS_params"]["keysFileName"]

        keysFile = os.path.join(self.keysFileDir, self.keysFileName)
        with open(keysFile, 'r') as keysJSONFile:
            esiosKeyData = json.load(keysJSONFile)
            self.__esiosKey = esiosKeyData["ESIOS_KEY"]    # We'll let this private

        self._indicadores: List[int] = data["ESIOS_params"]["indicadores"]
        self._time_trunc: str = data["ESIOS_params"]["time_trunc"]

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

    def reloadParamsFile(self):
        """Reloads the input parameters file. 
        Used in case the user has modified some input field (e.g. for adjustment) in the input file.
        """

        inputParamsFile = os.path.join(self._paramsFileDirectory, self._paramsFileName)
        self._loadParams(inputParamsFile)

    def _getDataForYear(self, year: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
        initialDate: datetime = datetime(year, 
                                         initDatetime.month, initDatetime.day, initDatetime.hour, initDatetime.minute)
        endDate: datetime = initialDate + timedelta(hours = hoursAhead)
        # print("inicio: " + str(inicio) + " - hasta: " + str(hasta))

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
        """Get the data from source considering the specified parameters. 
        If some parameter is not provided, the one from the input file is used by default.

        :param int initialYear: first year considered for the request.
        :param datetime initDatetime: the initial (MM-DD-hh-mm) considered for the request.
        :param int hoursAhead: hours from 'initDatetime' on that we want to consider for the request.
        :returns pandas.DataFrame:
        """
        
        if not initialYear: initialYear = self.initialYear
        if not hoursAhead: hoursAhead = self.hoursAhead

        esiosInstance = self.bibliotecaEsios.BajadaDatosESIOS(self.__esiosKey)

        df: pd.DataFrame = self._getDataForFirstYear(initialYear, initDatetime, hoursAhead, esiosInstance)
        df = self._getDataForTheRestOfYears(df, initialYear, initDatetime, hoursAhead, esiosInstance)

        # Shift the index (row names) to the current date
        df.index = df.index + pd.offsets.DateOffset(years = initDatetime.year - initialYear)

        return df
    
    def _checkDataFrameContiguity(self, df: pd.DataFrame):
        years: List = list(df.columns)
        print(years)

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
            df[element] = df[element] * adjustmentsDict[element]
        
        return df
    
    def resampleOnAxis0(self, df: pd.DataFrame, sampleFreqInMins: int = None, method: str = None, **kwargs) -> pd.DataFrame:
        """Resamples the current dataframe by rows, considering the resampling frequency, method and spline order for interpolation.
        It uses the pandas.DataFrame.interpolate(method, splineOrder) method.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the resampling should be applied.
        :param int sampleFreqInMins: the required output frequency.
        :param int method: the method by means of which the resampling will be performed. For 'polynomial' and 'spline' an 'order' must be specified in \*\*kwargs.
        :param *optional* ``kwargs``: keyword arguments to pass on to the interpolation function.
        :returns pandas.DataFrame:
        """

        if not sampleFreqInMins: sampleFreqInMins = self.resampling_sampleFreqInMins
        if not method: method = self.resampling_method

        polynomialMethods: List[str] = ["polynomial", "spline"]
        acceptedInterpolationMethods: List[str] = [*polynomialMethods]

        if method in polynomialMethods:
            order: int = self.resampling_splineOrder
            if "order" in kwargs:
                order = kwargs["order"]

            return self.bibliotecaGeneral.resampleaDataFrame(df, sampleFreqInMins, method, order)
        else:
            raise ValueError("Interpolation method '" + method + "' not implemented. Please choose some: " + ', '.join(acceptedInterpolationMethods) + ".")
    
    def _checkFrequencyFormatIsValid(self, frequency: str):
        if not re.match("(\d+)([DWMHTS])", frequency):
            raise ValueError("Frequency '" + frequency + "' not valid. It should and integer followed by a unit ('M':monthly, 'W': weekly, 'D': daily, 'H': hourly, 'T': minutely, 'S': secondly). E.g. \"2T\" == and entry for every 2 minutes.")

    def _checkFinerDFResolution(self, df: pd.DataFrame, frequency: str):
        dfFreq: str = pd.infer_freq(df.index)
        
        dfDelta: pd.Timedelta = pd.Timedelta(dfFreq)
        freqDelta: pd.Timedelta = pd.Timedelta(frequency)

        if freqDelta < dfDelta:
            raise ValueError("The provided frequency '" + frequency + "' is of a finer resolution than the one of the DataFrame ('" + dfFreq + "'). Please, choose a coarser one for the data to be aggregated into.")

    def downsample(self, df: pd.DataFrame, frequency: str = None, aggregationFunc: function | str = None) -> pd.DataFrame:
        """Aggregates the DataFrame by means of an aggregation function, getting a new DataFrame with the specified frequency (which must be coarser-grained).
        It uses the pandas.Dataframe.resample(rule) & the pandas.core.resample.Resampler.aggregate(func) methods.
        If some parameter is not provided, the one from the input file is used by default.

        :param pandas.DataFrame df: the DataFrame to which the downsampling should be applied.
        :param str frequency: the resulting frequency under which the DataFrame should be aggregated
        :param function | str aggregationFunc: a function (e.g. lambda x: x.mean()) or a string (e.g. "mean") representing the aggregation function to be applied
        :returns pandas.DataFrame:
        """

        self._checkFrequencyFormatIsValid(frequency)
        self._checkFinerDFResolution(df, frequency)

        if not aggregationFunc: aggregationFunc = self.downsampling_aggreationFunc
        if not frequency: frequency = self.downsampling_frequency

        return df.resample(frequency).agg(aggregationFunc)