# -*- coding: utf-8 -*-
"""Algunas utilidades de proposito general 

 Tras main: algunos ejemplos de utilización

"""
import pandas as pd
from scipy.stats import truncnorm
from collections import OrderedDict


def calculaCorrecionDePreciosPorIPC(unDiccionarioAgnoIPC):
    """ Devuelve diccionario AÑO: IPC en %" para determinar el valor actual de un importe
    
    Asumamos que pagamos por un bien 100 euros el 1/1/2008, y la fecha actual es 1/1/2011
    ¿cuanto equivale ese capital a día de hoy? Para el su calculo necesitamos una tabla (diccionario)
    -entrada a esta función- que nos de el IPC para cada año comprendido entre el año de la inverión
    y el año actual.
    Por ejemplo unDiccionarioAgnoIPC = { 2008: 10, 2009:20, 2010:30 } # ES DECIR IPC 2008 10%, IPC 2009 20% etc
    para calcular la evolución del valor del capital
      - 100 Euros el 1 de Enero 2008, acorde al diccionario anterior se calcula:
      a) 100 + 100*0.1 Euros el 1 de Enero 2009 == equivalen a 100 * ( 1 + 10/100 ) = 110
      b) 110 + 100*0.2 Euros el 1 de Enero de 2010 = 110 * (1 + 20/200) = [100 * ( 1 + 10/100 )] * (1 + 20/200)]
      
    etc ==> ya se ve claramente la lógica.

    La salida de esta función es un diccionario "AÑO: coeficiente", donde coeficiente es el número que multiplicado
    por un capital de ese AÑO, da el valor actualizado del capital
    
    Ejemplo:
       calculaCorrecionDePreciosPorIPC( unDiccionarioAgnoIPC={2010: 40, 2009: 30, 
                                                              2008: 20, 2007: 10}
       devuelve: {2010: 1.4, 2009: 1.8199999999999998, 2008: 2.1839999999999997, 2007: 2.4024}       
        
        
    :param unDiccionarioAgnoIPC: Un diccionario clave: AÑO valor:IPC intranual en tanto por ciento
        
    :raises: :class: ` Runtime Error `: con esta sintaxís puede navegar a este objeto
        
    :returns: out
    :rtype: diccionario 
    """
    

# Como el usuario es como es, no asumimos que el diccionario es pasado en orden
# Como primer paso: creamos un diccionario ordenado en orden inverso de claves
# ejemplo: 
# diccionarioAgnoIPC= {   
#    2010: 40,
#    2008: 20,     
#    2007: 10,
#    2009: 30
# }
# 
# al correr la lógica adjunta.
# diccionarioOrdenadoIPC={
#    2010: 40,
#    2009: 30,
#    2008: 20,     
#    2007: 10,
# }
    listaAgnos = list(unDiccionarioAgnoIPC.keys())
    listaAgnos.sort(reverse=True)                      # En orden inverso
    diccionarioOrdenadoIPC = OrderedDict({i: unDiccionarioAgnoIPC[i] for i in listaAgnos})

    correccion_IPC = {}   
    valorAcumulado = 1
    for key, value in diccionarioOrdenadoIPC.items():
       valorAcumulado = valorAcumulado * ( 1 + value/100 )
       correccion_IPC[key] = valorAcumulado
        
    return correccion_IPC

def resampleaDataFrame(df, frecuenciaSampleo, method='polynomial', order=2):
    """ Resamplea un dataframe, en el que index es un datetime, a la frecuenciSampleoMinutos pasada

    Importante: lo parámetros method y order son los mismo que el método interpolate de la clase DataFrame
    (en particular method='polynomial' order='2' resampelo por splines de orden 2)
    Ejemplo:
       # PRIMER PARTE: Generar un data frame dummy 
       mes = pd.date_range('01-01-2000', '01-02-2000', freq='H')
       datos = list(range(1,len(mes)+1,1))
       datos2 = [i*2 for i in datos]
       df = pd.DataFrame(list(zip( mes, datos, datos2)),  columns=['mes', 'uno', 'dos'])
       df.set_index(['mes'], inplace=True)
       # Contenido de df (trozo)
         #  Index                uno dos
         #  2000-01-01 00:00:00  1   2
         #  2000-01-01 01:00:00  2   4
         #  2000-01-01 02:00:00  3   6
         #  2000-01-01 03:00:00  4   8
         #  2000-01-01 04:00:00  5   10
       # llamada
       df = resampleaDataFrame(df, 15)
       # df vale ahora
         #  Index                uno dos
         #  2000-01-01 00:00:00   1.00  2.0
         #  2000-01-01 00:15:00   1.25  2.5
         #  2000-01-01 00:30:00   1.50  3.0
         #  2000-01-01 00:45:00   1.75  3.5
         #  2000-01-01 01:00:00   2.00  4.0
         #  ...	...	...
         #  2000-01-01 23:00:00   24.00  48.0
         #  2000-01-01 23:15:00   24.25  48.5
         #  2000-01-01 23:30:00   24.50  49.0
         #  2000-01-01 23:45:00   24.75  49.5
         #  2000-01-02 00:00:00   25.00  50.0
       
        
    :param df: Un dataframe con index un datetimeIndex
    :param frecuenciaSampleo: un string con la frecuencia de sampleo que se desea (ej. "2T" === 2 minutos)
    :param method: Método de interpolación (ver ayuda de df.interpolate)
    :param order: Parámetro adicional para la interpolación  (ver ayuda de df.interpolate)
    
    :returns: df resampleado
    :rtype: diccionario 
    """
    
    primerValor = df.index[0]
    ultimoValor = df.index[-1] 

    # SampleoDeseado, es un dataframe con indice timeStamp con sampleo deseado
    sampleoDeseado = pd.date_range(primerValor, ultimoValor, freq=frecuenciaSampleo)
    df = df.reindex(df.index.union(sampleoDeseado))
    df =df.interpolate(method=method, order=order)

    return df


        
if __name__ == "__main__":
     
# Comportamiento en función del tipo de ejecución    

    tipo_ejecucion = 1 # Ejemplo de uso calculaCorrecionDePreciosPorIPC
   # tipo_ejecucion = 2 # Ejemplo de uso resampleaDataFrame    
    
    
    if tipo_ejecucion == 1:
        diccionario = unDiccionarioAgnoIPC={2010: 40, 2009: 30, 2008: 20, 2007: 10}
        out = calculaCorrecionDePreciosPorIPC(diccionario)
        print(out) 
    if tipo_ejecucion == 2:
       ####Primera parte: se genera un dataframe con campo indice un timeStampo 
       mes = pd.date_range('01-01-2000', '01-02-2000', freq='H')
       datos = list(range(1,len(mes)+1,1))
       datos2 = [i*2 for i in datos]
       df = pd.DataFrame(list(zip( mes, datos, datos2)),  columns=['mes', 'uno', 'dos'])
       df.set_index(['mes'], inplace=True)
       # Contenido de df (trozo)
         #  Index                uno dos
         #  2000-01-01 00:00:00  1   2
         #  2000-01-01 01:00:00  2   4
         #  2000-01-01 02:00:00  3   6
         #  2000-01-01 03:00:00  4   8
         #  2000-01-01 04:00:00  5   10
       # llamada
       df = resampleaDataFrame(df, 15)
       # df vale ahora
         #  Index                uno dos
         #  2000-01-01 00:00:00   1.00  2.0
         #  2000-01-01 00:15:00   1.25  2.5
         #  2000-01-01 00:30:00   1.50  3.0
         #  2000-01-01 00:45:00   1.75  3.5
         #  2000-01-01 01:00:00   2.00  4.0
         #  ...	...	...
         #  2000-01-01 23:00:00   24.00  48.0
         #  2000-01-01 23:15:00   24.25  48.5
         #  2000-01-01 23:30:00   24.50  49.0
         #  2000-01-01 23:45:00   24.75  49.5
         #  2000-01-02 00:00:00   25.00  50.0