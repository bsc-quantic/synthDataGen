import os
import json
import inspect

from importlib.resources import files
from typing import List

import pandas as pd
from datetime import datetime, timedelta


class LoaderInterface:

    indexName = "datetime"

    def __init__(self, paramsFileName: str):
        """Loads the main parameters from the specified input JSON.

        May be also useful to reload the input parameters file at any time during the execution.

        :param str paramsFileName: the name of the JSON file containing the dictionary (absolute or relative path) with the loader params
        """
        raise NotImplementedError

    @property
    def dataSourceOptions(self):
        return self._dataSourceOptions

    @property
    def dataSource(self):
        return self._dataSource

    @dataSourceOptions.setter
    def dataSourceOptions(self, new_dataSourceOptions: str):
        self._dataSourceOptions = new_dataSourceOptions

    @dataSource.setter
    def dataSource(self, new_dataSource: str):
        self._dataSource = new_dataSource

    def _getJSON(self, fileName: str) -> List:
        if not os.path.isfile(fileName):
            raise Exception("File '" + fileName + "' does not exist.")

        with open(fileName, 'r') as jsonFile:
            return json.load(jsonFile)

    def getDataFromSource(self, initialYear: int, initDatetime: datetime = datetime.now(), hoursAhead: int = 10, include29February = False) -> pd.DataFrame:
        """Get the data from source considering the specified parameters. 
        If some parameter is not provided, the one from the input file is used by default.

        :param int initialYear: first year considered for the request.
        :param datetime initDatetime: the initial (MM-DD-hh-mm) considered for the request.
        :param int hoursAhead: hours from 'initDatetime' on that we want to consider for the request.
        :param bool include29February: indicates whether or not to include the February 29 in the returned DataFrame.
        :returns pandas.DataFrame:
        """
        raise NotImplementedError

class ESIOSLoader(LoaderInterface):

    from synthDataGen.common import bibliotecaEsios

    def __init__(self, paramsFileName: str):
        data = self._getJSON(paramsFileName)

        self._keysFileDir: str = data["ESIOS_params"]["keysFileDir"]
        self._keysFileName: str = data["ESIOS_params"]["keysFileName"]

        moduleName: str = self._getPackageName()
        keysFile: str = os.path.join(self.keysFileDir, self.keysFileName)
        keysJSONFile = files(moduleName).joinpath(keysFile).read_text()

        self.__esiosKey = json.loads(keysJSONFile)["ESIOS_KEY"]

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

    def _getPackageName(self):
        if __package__:
            return __package__
        elif '__main__' in __name__:
            return inspect.currentframe().f_globals['__name__'].split('.')[0]
        else:
            return __name__.split('.')[0]

    def _isLeapYear(self, year: int):
        return ((year % 400 == 0) or (year % 100 != 0) and
                                     (year % 4 == 0))

    def _getDataForYear(self, year: int, initDatetime: datetime, hoursAhead: int, include29February: bool, esios) -> pd.DataFrame:
        initialDate: datetime = datetime(year, 
                                        initDatetime.month, initDatetime.day, initDatetime.hour, initDatetime.minute)
        endDate: datetime = initialDate + timedelta(hours = hoursAhead)

        if not include29February and self._isLeapYear(year) and \
           initialDate <= datetime(year, 2, 28) <= endDate and \
           endDate.date() != initialDate.date():
            endDate = endDate + timedelta(hours=24)

        return esios.dataframe_lista_de_indicadores_de_esios_por_fechas([self.indicador], 
                                                                        initialDate, endDate, include29February,
                                                                        time_trunc = self.time_trunc).filter(["value"], axis = 1)

    def _getDataForFirstYear(self, initialYear: int, initDatetime: datetime, hoursAhead: int, include29February: bool, esios) -> pd.DataFrame:
        return self._getDataForYear(initialYear, initDatetime, hoursAhead, include29February, esios).rename(columns = {"value": initialYear})

    def _getDataForTheRestOfYears(self, df: pd.DataFrame, initialYear: int, initDatetime: datetime, hoursAhead: int, include29February: bool, esios) -> pd.DataFrame:
        for year in range(initialYear + 1, datetime.now().year):
            df[year] = list(self._getDataForYear(year, initDatetime, hoursAhead, include29February, esios)["value"])

        return df

    def getDataFromSource(self, initialYear: int, initDatetime: datetime, hoursAhead: int, include29February: bool = False) -> pd.DataFrame:
        esiosInstance = self.bibliotecaEsios.BajadaDatosESIOS(self.__esiosKey)

        df: pd.DataFrame = self._getDataForFirstYear(initialYear, initDatetime, hoursAhead, include29February, esiosInstance)
        df = self._getDataForTheRestOfYears(df, initialYear, initDatetime, hoursAhead, include29February, esiosInstance)

        # Shift the index (row names) to the current date
        df.index = df.index + pd.offsets.DateOffset(years = initDatetime.year - initialYear)

        df.rename_axis(self.indexName, inplace=True)

        return df
    
class LocalDFLoader(LoaderInterface):

    def __init__(self, paramsFileName: str):
        data = self._getJSON(paramsFileName)

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
    def columnToAnalyze(self):
        return self._columnToAnalyze

    @property
    def skipFirstColumn(self):
        return self._skipFirstColumn

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

    @columnToAnalyze.setter
    def columnToAnalyze(self, new_columnToAnalyze: str):
        self._columnToAnalyze = new_columnToAnalyze

    @skipFirstColumn.setter
    def skipFirstColumn(self, new_skipFirstColumn: str):
        self._skipFirstColumn = new_skipFirstColumn
    
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

        return resultDF

    def __setIndexAndFilter29February(self, df: pd.DataFrame, include29February: bool) -> pd.DataFrame:
        df = df.rename({str(entry):"2024" + "-" + str(entry) for entry in df.index})      # This mental retardation is for pandas to not check the February 29th when parsing the index to datetime
        df.index = pd.to_datetime(df.index)

        if not include29February:
            df = df[~((df.index.month == 2) & (df.index.day == 29))]

        if len(set(df.index.year)) == 1:
            df.index = df.index + pd.offsets.DateOffset(years = 2023 - df.index.year[1])

        df.rename_axis(self.indexName, inplace=True)

        return df

    def __filterInNeededData(self, df: pd.DataFrame, initialYear: int, initDatetime: datetime, hoursAhead: int) -> pd.DataFrame:
        minYear: int = int(min(list(df.columns)))

        if initialYear > minYear:
            df.drop(columns = [str(year) for year in range(minYear, initialYear)], inplace = True)
        
        listOfHoursAhead = [initDatetime + timedelta(hours=hour) for hour in range(0, hoursAhead + 1)]
        df1 = df.filter(items = listOfHoursAhead, axis = 0)

        if len(df) == 0:
            raise Exception("initDateTime '" + str(initDatetime) + "'is not a valid row for the current DataFrame.")
        
        return df1

    def getDataFromSource(self, initialYear: int, initDatetime: datetime, hoursAhead: int, include29February: bool = False) -> pd.DataFrame:
        df = pd.read_csv(self.dataFrameFile)

        if self.skipFirstColumn:
            df = df.iloc[:, 1:]

        self.__createDateColumns(df)
        df = self.__rearrangeDFByYear(df)

        df = self.__setIndexAndFilter29February(df, include29February)
        df = self.__filterInNeededData(df, initialYear, initDatetime, hoursAhead)
        return df