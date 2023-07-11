import os
import json

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
        self._adjustmentByYear: Dict[int, int] = [(int(key), value) for key, value in data["adjustmentByYear"].items()]

        # Resampling parameters
        self._resampling_sampleFreqInMins: int = data["resampling_params"]["sampleFreqInMins"]
        self._resampling_method: str = data["resampling_params"]["method"]
        self._resampling_splineOrder: int = data["resampling_params"]["splineOrder"]

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

    from bibliotecaEsios import BajadaDatosESIOS

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

    def _getDataForYear(self, initialYear: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
        initialDate: datetime = datetime(initialYear, 
                                         initDatetime.month, initDatetime.day, initDatetime.hour, initDatetime.minute)
        endDate: datetime + timedelta(hours = hoursAhead)
        # print("inicio: " + str(inicio) + " - hasta: " + str(hasta))

        df = esios.dataframe_lista_de_indicadores_de_esios_por_fechas(self.indicadores, 
                                                                  initialDate, endDate, 
                                                                  time_trunc = self.time_trunc,
                                                                  inlcuye29DeFebrero = '').filter(["value"], axis = 1)

        return df.rename(columns = {"value": str(initialYear)})

    def _getDataForTheRestOfYears(self, df: pd.DataFrame, initialYear: int, initDatetime: datetime, hoursAhead: int, esios) -> pd.DataFrame:
        for year in range(initialYear + 1, datetime.now()):
            df[str(year)] = list(self._getDataForYear(esios, initialYear, initDatetime, hoursAhead)["value"])

        return df

    def getDataFromSource(self, initialYear: int = None, initDatetime: datetime = datetime.now(), hoursAhead: int = None) -> pd.DataFrame:
        """Get the data from source considering the specified parameters. 
        If the parameters are not provided, the ones from the input file are used by default.

        :param int initialYear: first year considered for the request
        :param datetime initDatetime: the initial (MM-DD-hh-mm) considered for the request
        :param int hoursAhead: hours from 'initDatetime' on that we want to consider for the request
        """
        
        if not initialYear: initialYear = self.initialYear
        if not hoursAhead: hoursAhead = self.hoursAhead

        esiosInstance = self.BajadaDatosESIOS(self.__esiosKey)

        df: pd.DataFrame = self._getDataForYear(initialYear, initDatetime, hoursAhead, esiosInstance)
        df = self._getDataForTheRestOfYears(df, initialYear, initDatetime, hoursAhead, esiosInstance)

        # df.index = df.index + pd.offsets.DateOffset(years = initDatetime.year - self.initialYear)

        return df
    
    def _checkDataFrameContiguity(self, df: pd.DataFrame):

        return None

    def _checkAdjustmentsDict(self, initialYear: int, adjustmentsDict: Dict):
        years_adjustmentsDict = sorted([year for year in adjustmentsDict.keys()])
        years_fromInitialYear = sorted([year for year in range(initialYear, datetime.now().year)])

        if years_adjustmentsDict != years_fromInitialYear:
            raise ValueError("Not valid adjustments dictionary. It must contain an entry for every year since the first year in the provided DataFrame!")

    def performAnualAdjustments(self, df: pd.DataFrame, adjustmentsDict: Dict = None) -> pd.DataFrame:
        """Performs an anual adjustments on the current dataframe with the provided dictionary.
        If the parameters are not provided, the ones from the input file are used by default.

        :param dict adjustmentsDict: dictionary of percentages of adjustment by year
        """

        if not adjustmentsDict: adjustmentsDict = self.adjustmentsDict

        self._checkDataFrameContiguity(df)
        self._checkAdjustmentsDict(min(df.columns), adjustmentsDict)

        for element in df.columns:
            df[element] = df[element] * adjustmentsDict[element]
        
        return df
    
    def resampleOnAxis0(self, sampleFreqInMins: int = None, method: str = None, splineOrder: int = None):
        """Resamples the current dataframe by rows, considering the resampling frequency, method and spline order for interpolation.
        If the parameters are not provided, the ones from the input file are used by default.

        :param int sampleFreqInMins:
        :param int method:
        :param int splineOrder:
        """

        if not sampleFreqInMins: sampleFreqInMins = self.sampleFreqInMins
        if not method: method = self.method
        if not splineOrder: splineOrder = self.splineOrder

        return None
    
    def resampleOnAxis1(self):

        return None