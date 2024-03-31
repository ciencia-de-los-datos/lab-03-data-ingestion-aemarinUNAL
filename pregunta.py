"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd
import re

def ingest_data():

    file_path = r'clusters_report.txt'

    widths = [9, 16, 16, 80]

    df_clusters = pd.read_fwf(
        file_path,
        colspecs='infer',
        
        widths=widths,
        #skiprows=skiprows,
        #skipfooter=skipfooter,
        header=None,
        names=['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave'],
        converters={'porcentaje_de_palabras_clave': lambda x: x.rstrip("%").replace(",", ".")}
    ).drop(index={0,1,2}).ffill()
    
    df_consolidado = df_clusters.groupby(
            ['cluster'], 
            as_index=False
        ).agg(
            {
            'cantidad_de_palabras_clave': 'first', 
            'porcentaje_de_palabras_clave': 'first', 
            'principales_palabras_clave': lambda x: ' '.join(x) 
            }
        )
    df_consolidado['cluster'] = df_consolidado['cluster'].astype('int')
    df_consolidado['cantidad_de_palabras_clave'] = df_consolidado['cantidad_de_palabras_clave'].astype('int')
    df_consolidado['porcentaje_de_palabras_clave'] = df_consolidado['porcentaje_de_palabras_clave'].astype('float')
    df_consolidado['principales_palabras_clave'] = df_consolidado['principales_palabras_clave'].str.replace(r'\s+', ' ', regex=True).str.replace('.', '')
    #df_consolidado['principales_palabras_clave'] = df_consolidado['principales_palabras_clave'].apply(limpiar_espacios)
    df_consolidado=df_consolidado.drop_duplicates(subset=['cluster']).sort_values('cluster').reset_index(drop=True)
    df_final = df_consolidado

    return df_final

print(ingest_data())
