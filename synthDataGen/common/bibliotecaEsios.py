# -*- coding: utf-8 -*-

"""Clase trivial para interacción con ESIOS 

Trata de facilitar al usuario una interacción elemental con ESIOS
Clase: BajadaDatosESIOS
Sobre modo de utilización: En este mismo fichero, hay varios ejemplos "sencillos" (tras main) ==>
Esta biblioteca puede ejecutarse de forma "autonóma" sin necesidad de ser importada en código

Es una adaptación de funcionalida encontrada en:
https://github.com/mharias/mercado_electrico/blob/main/code/notebook_uso_apis.ipynb
"""
from __future__ import division
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil import tz
import requests    # Posiblemente, tengamos que instalar previamente con pip
import html

from dateutil import parser



class BajadaDatosESIOS:
    """ Facilita la interacción con ESIOS
    
    Es una adaptación de funcionalidad encontrada en:
    https://github.com/mharias/mercado_electrico/blob/main/code/notebook_uso_apis.ipynb

  
    :param token: Es el token del usuario de ESIOS (se solicita en la propia web)
    :type  cadena: Es una cadena de caracteres 
    

    """
    def __init__(self, token):
        """ Creación de una instancia
        
        Esencialmente, aprovecha la llamada a exios que solicita la lista de items que pone a disposición
        para crear el objeto y verificar que el token con el que se llama, es un token valido
        
        :param token:  Es el token del usuario de ESIOS (se solicita en la propia web)
        :type  cadena: Es una cadena de caracteres         
        
        :raises: :class: ` Runtime Error `: No mucho esfuerzo: Error genérico por no poder haber hecho conexión
        

        """
        self.__token = token
        self.__headers = {'Accept':'application/json; application/vnd.esios-api-v2+json',           
                          'Content-Type':'application/json',
                          'Host':'api.esios.ree.es',
                          'x-api-key': self.__token,     # Cambio respecto a fuente de inspiración
                          'Cache-Control': 'no-cache',
                          'Pragma': 'no-cache'          
                          }
        self.__end_point = 'https://api.esios.ree.es/indicators'
        self.__response = requests.get(self.__end_point, headers=self.__headers).json()
        self.__df = pd.DataFrame()
        self.__volteado = None
        
    #del resultado en json bruto se convierte a pandas dataframe, y se eliminan los tags del campo description

        self.__lista_de_items = pd.json_normalize(data=self.__response['indicators'], errors='ignore') \
            .assign(description = lambda df_: df_.apply(lambda df__: html.unescape(df__['description']
                                                                  .replace('<p>','')
                                                                  .replace('</p>','')
                                                                  .replace('<b>','')
                                                                  .replace('</b>','')), 
                                                        axis=1) 
                   )
             
                                          
          
    @property    
    def data_frame_catalogo_items(self):
        """ Devuelve un dataframe con los indicadores que ESIOS pone a disposición para consulta  

        """  
        return self.__lista_de_items 
    
    @property
    def df(self):
        """Devuelve el full dataframe
        """
        return self.__df    
    
    @property    
    def data_frame_volteado(self):
        """ Devuelve un dataframe muy interesante para analisis. Es un cambio de representación de la info
            base, segregando por filas cada día y por columnas, cada hora

        """  
        if self.__volteado == None:
            self.__volteado = self.__df.set_index(
                            [self.__df.index.date, self.__df.index.time]
                             ).value.rename_axis([None] * 2).unstack()
        
        return self.__volteado 
    
    
    
    def dataframe_lista_de_indicadores_de_esios_por_fechas(self, indicadores,
                                                           fecha_inicio,
                                                           fecha_fin,
                                                           incluye29DeFebrero,
                                                           time_trunc='five_minutes'):
        """ Descarga un dataframe por fechas (desde/hasta( en UTC para una lista de indicadores ESIOS
        
        Devuelve un dataframe de pandas, pero ojo: el Dataframe es del tipo DatetimeIndex, (el indice es la serie
        temporal entre fechas incluidas). Esto facilita notablemente el tratamiento posterior (slicing y filtrado)
         Importante:  
          El indice está en UTC con lo cual: no hay lógica asociada a tener que trabajr con husos horarios 
        
        :param indicadores:  Lista de valores enteros correspondientes a los id de los indicadores a descargar (se consultan en ESIOS)
        
        :param fecha_inicio: Objeto datetime que contiene tiempo origen de la info que se deea descargar. ¡¡ en UTC !!
                Varias opciones:
                         inicio =  datetime(2023, 2, 27)         == 0 horas de ese dia 
                         inicio =  datetime(2023, 2, 27, 10, 30) == una hora concreta de ese día
           
        :param fecha_fin: Objeto datetime que contiene tiempo fin de la info que se deea descargar. ¡¡ en UTC !!
              Varias opciones:
                         fin =  datetime(2023, 2, 27)         == 23:59:00 del día 27 de febrero
                         fin =  datetime(2023, 2, 27, 10, 30) == una hora concreta de ese día      
                         
        :param time_trunc: Granularidad con el que consigue la opción. NO MUY CLARO == pte consulta equipo
              Algunas opciones encontradas en la propia web donde el código fue fusilado:
                         'days'  
                         'five_minutes'
                         
        :param inlcuye29DeFebrero: Para control valores de 29 de febrero 
                         SI NO BLANCO = Datos de 29 de febrero SON retornados   
                         SI BLANCO    = Datos 29 de febrero NO son retornados                             
                          
        :raises: :class: ` Runtime Error `: No mucho esfuerzo: Error genérico por no poder haber hecho conexión
        

        """
        '''  OLD CODE
        # Ajuste de horas para selección acorde a criterios [desde, hasta] - intervalo cerrado -
        fecha_inicio = fecha_inicio + timedelta(hours=1)
        fecha_fin = fecha_fin + timedelta(hours=1)       
        
        inicio = fecha_inicio.strftime('%Y-%m-%dT%H:%M')

        # Si la fecha fin no tiene hora/minutos/segundos, interpretamos que la hora de la fecha limite seleccionada es
        # 23:59:00

        if fecha_fin.hour == 0 & fecha_fin.minute == 0:
            fecha_fin_real = datetime( fecha_fin.year,
                                       fecha_fin.month,
                                       fecha_fin.day,
                                       23,
                                       59,
                                       59) 
            fin = fecha_fin_real.strftime('%Y-%m-%dT%H:%M')
        else:
            fecha_fin_real = fecha_fin
            fin = fecha_fin.strftime('%Y-%m-%dT%H:%M')
    '''
    
        # NEW-CODE: Más elegente, jugando con funcionalida datetime
        # Los valores de tiempo origen/destino de selección entran en este método en UTC
        # el snippet (fusilado de internet) que toma datos desde ESIOS, selecciona huso horario peninsular

        HUSO_PENINSULA = tz.gettz("Europe/Madrid")  
        HUSO_UTC = tz.gettz("UTC")   

        fecha_inicio_UTC =  fecha_inicio.replace(tzinfo=HUSO_UTC)   
        fecha_fin_UTC = fecha_fin.replace(tzinfo=HUSO_UTC) 

        fecha_inicio_PENINSULAR = fecha_inicio_UTC.astimezone(HUSO_PENINSULA)
        fecha_fin_PENINSULAR = fecha_fin_UTC.astimezone(HUSO_PENINSULA)

     
        inicio = fecha_inicio_PENINSULAR.strftime('%Y-%m-%dT%H:%M')
        fin = fecha_fin_PENINSULAR.strftime('%Y-%m-%dT%H:%M')        
        # FIN NEW-CODE
           
    # El procedimiento es sencillo: 
    # a) por cada uno de los indicadores configuraremos la url, según las indicaciones de la documentación.
    # b) Hacemos la llamada y recogemos los datos en formato json.
    # c) Añadimos la información a una lista   
        lista=[]
        for indicador in indicadores:
            url = f'{self.__end_point}/{indicador}?start_date={inicio}&end_date={fin}&time_trunc={time_trunc}'            
            response = requests.get(url, headers=self.__headers).json()
            lista.append(pd.json_normalize(data=response['indicator'], record_path=['values'], meta=['name','short_name'], errors='ignore'))
    # DE LA FUENTE QUE SE FUSILÓ:
    # Devolvemos como salida de la función un df fruto de la concatenación de los elemenos de la lista
    # Este procedimiento, con una sola concatenación al final, es mucho más eficiente que hacer múltiples 
    # concatenaciones.
    # Adaptación adicional: 
    # el indice el dataframe es un datetimeIndex, que como es sabido facilita sensiblemente
    #                       el tratamietno de la información de la serie temporal con posterioridad
    # A FINES PRACTICOS: TRABAJAR CON UTC
    #  1) El indice se determina a partir de la columa "datetime_utc"
        df = pd.concat(lista, ignore_index=True)
          # La línea siguiente, se realiza en tres pasos:
          # columna_aux = df['datetime_utc']
          # neoColumna = np.array( datetime.strptime(a, '%Y-%m-%dT%H:%M:%SZ') for a in columna_aux)
          # df['datetime_utc'] = neoColumna
          # que puedes condensarse:

        df["datetime_utc_norm"] = pd.to_datetime(df["datetime_utc"], format='ISO8601')
        df['datetime_utc'] = np.array([date.replace(microsecond = 0).tz_localize(None) for date in df["datetime_utc_norm"]])
        # df['datetime_utc'] = np.array( datetime.strptime(a, '%Y-%m-%dT%H:%M:%SZ') for a in df['datetime_utc'])  

        
        df.index = pd.to_datetime(df.datetime_utc)

        df = df[(df.index >= fecha_inicio) & (df.index <= fecha_fin)]
    #  2) Eliminamos el 29 de febrero, en caso de ser así requerido 
        # if inlcuye29DeFebrero.strip() != '':                        # Old code                  
        #    df = df[(df.index.day != 29) & (df.index.month != 2) ]   # Old code
        if not incluye29DeFebrero:
        # if not(incluye29DeFebrero.strip() != ''):                     # Neo code
            df = df[~((df.index.day == 29) & (df.index.month == 2))]  # Neo code
        self.__df = df
        return self.__df    
    
    
        
        
if __name__ == '__main__':
    try:
      from ficheroDeClaves import CLAVEdeESIOS
    except:
      cadenaError = '\n '
      cadenaError = '\n Con objeto de que el código sea lo más "privado" posible'
      cadenaError += '\n para ser ejecutado en modo demo '
      cadenaError += '\n crear un fichero llamado ficheroDeClaves.py'
      cadenaError += '\n y dentro del mismo definir la variable CLAVEdeESIOS = el ID que se posea para consultas ESIOS'
      raise Exception(cadenaError) 


# Comportamiento en función del tipo de ejecución       
    #tipo_ejecucion = 1  # data_frame_catalogo_items + df de resultados
    #tipo_ejecucion = 2  #  Ejemplo trivial de selección desde cero horas de un día, hasta 23:59:00 de otro        
    # tipo_ejecucion = 3  #  Ejemplo entre horas dadas de dias          
    #tipo_ejecucion = 4  #  2020 fue bisiesto. Llamada RETORNO SIN 29 DE FEBRERO         
    #tipo_ejecucion = 5  #  2020 fue bisiesto. Llamada RETORNO CON 29 DE FEBRERO    
    #tipo_ejecucion = 6  #  eL 2022 MES 3 DIA 27 HUBO CAMBIO DE HORA, como trabajamos en UTC: No problemas
    tipo_ejecucion = 7  # Descarga masiva/generación BASE DE DATOS
    
    if tipo_ejecucion == 1:  # data_frame_catalogo_items
        a =  BajadaDatosESIOS(CLAVEdeESIOS)
        dataFrameDetalleDeItems = a.data_frame_catalogo_items
        print(dataFrameDetalleDeItems.head())
       
        
    if tipo_ejecucion == 2:  #  Ejemplo trivial de selección desde cero horas de un día, hasta 23:59:00 de otro
        a =  BajadaDatosESIOS(CLAVEdeESIOS)
        inicio = datetime(2020, 1, 27)
        fin = datetime(2020, 1, 28)

        #df = a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, time_trunc='five_minutes') 
        a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, time_trunc='five_minutes') 
        
        print('************** Primer registro ==> OJO que HAY MÁS modo demo***************************')
        print(a.df.iloc[0])
        print(' ')
        print('************** Ultimo registro ***************************')       
        print(a.df.iloc[-1])   
        
    if tipo_ejecucion == 3:  #  Ejemplo entre horas dadas de dias 
        a =  BajadaDatosESIOS(CLAVEdeESIOS)
        inicio = datetime(2020, 1, 27, 20, 15)  # desde 2020-01-27 a los 20 horas y 15 = 21 horas !!!
        fin = datetime(2020, 1, 28, 10)         # hasta 2020-01-28 a los 10 horas

        a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, time_trunc='five_minutes') 
        
        print('************** Primer registro ==> OJO HAY MÁS ***************************')
        print(a.df.iloc[0])
        print(' ')
        print('************** Ultimo registro ***************************')       
        print(a.df.iloc[-1])          
        
    if tipo_ejecucion == 4:  #  2020 fue bisiesto. Llamada RETORNO SIN 29 DE FEBRERO
        a =  BajadaDatosESIOS(CLAVEdeESIOS)
        inicio = datetime(2020, 2, 27)      # desde comienzo del día
        fin =  datetime(2020, 3, 1, 4, 0)  # hasta la cuatro de la mañana

        a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, time_trunc='five_minutes') 
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(' Se adjunta lista de horas (sin 29 de febrero)  ')
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        _ = [print(hora) for hora in list(a.df.index)]
        
    if tipo_ejecucion == 5:  #  2020 fue bisiesto. Llamada RETORNO CON 29 DE FEBRERO
        a =  BajadaDatosESIOS(CLAVEdeESIOS)
        inicio = datetime(2020, 2, 27)      # desde comienzo del día
        fin =  datetime(2020, 3, 1, 4, 0)  # hasta la cuatro de la mañana

        a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, 
                                                                  time_trunc='five_minutes', inlcuye29DeFebrero = 'X') 
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(' Se adjunta lista de horas  (CON 29 de febrero)  ')
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        _ = [print(hora) for hora in list(a.df.index)]     
        
    if tipo_ejecucion == 6:  #  eL 2022 MES 3 DIA 27 HUBO CAMBIO DE HORA, como trabajamos en UTC: No problemas        
        # https://armada.defensa.gob.es/ArmadaPortal/page/Portal/ArmadaEspannola/cienciaobservatorio/prefLang-es/06Hora--02cambioshoraoficial
        a =  BajadaDatosESIOS(CLAVEdeESIOS)
        inicio = datetime(2022, 3, 26, 23)      # desde comienzo del día
        fin =  datetime(2022, 3, 27, 5, 0)  # hasta la cuatro de la mañana
        a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, time_trunc='five_minutes') 
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        print(' Se adjunta lista de horas   ')
        print('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        _ = [print(hora) for hora in list(a.df.index)]
  
  ## New Code
    if tipo_ejecucion == 7:  #  Bajada seie historica intervalo años
                             # Recordar: serie histórica desde 2007
        AGNODESDE = 2007
        AGNOHASTA = 2023 # 2023
        FICHERO_SALIDA = 'Datos_ESIOS_2007_2023_SIN_29s_DE_FEBRERO.csv'
                             
        # Comienza Código
        listaDeAgnos = range(AGNODESDE, AGNOHASTA + 1)
        dataFrameSalida = pd.DataFrame()
        
        for agnoEnCurso in listaDeAgnos:
            print(' ')
            print('---------- Procesando año ', agnoEnCurso)
            a =  BajadaDatosESIOS(CLAVEdeESIOS)
            inicio = datetime(agnoEnCurso, 1, 1, 0, 0)      # comienzo del año
            fin =  datetime(agnoEnCurso, 12, 31, 23, 0)    # fin del año
            a.dataframe_lista_de_indicadores_de_esios_por_fechas([10211], inicio, fin, time_trunc='five_minutes')
            dataFrameSalida = pd.concat([dataFrameSalida, a.df[['value']]], axis=0)
              
        dataFrameSalida.to_csv(FICHERO_SALIDA, sep=';')
        print('Terminado Proceso')
          