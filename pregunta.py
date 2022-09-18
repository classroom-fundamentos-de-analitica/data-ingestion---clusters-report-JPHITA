"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    df = pd.read_fwf(
        "clusters_report.txt",
        widths=[7, 16, 16, 78],
        header=[0],
        skiprows=[1, 2, 3]
    )

    lastCluster = 1
    lastPClave = 105
    lastPorcentaje = "15,9 %"
    for i, _ in df.iterrows():
        if df.iloc[i,0] != lastCluster and not pd.isna(df.iloc[i,0]):
            lastCluster = df.iloc[i,0]
            lastPClave = df.iloc[i,1]
            lastPorcentaje = df.iloc[i,2]
        else:
            df.iloc[i,0] = lastCluster
            df.iloc[i,1] = lastPClave
            df.iloc[i,2] = lastPorcentaje

    df = df.groupby(["Cluster", "Cantidad de", "Porcentaje de"])

    df = df.agg(lambda x: " ".join(x)).reset_index()

    # quitar espacios repetidos entre palabras
    df["Principales palabras clave"] = df["Principales palabras clave"].str.replace(r"\s{2,}", " ", regex=True)

    # quitar los puntos
    df["Principales palbras clave"] = df["Principales palabras clave"].str.replace(".", "")

    # arreglar los porcentajes
    df["Porcentaje de"] = df["Porcentaje de"].str.slice(0, -2)
    df["Porcentaje de"] = df["Porcentaje de"].str.replace(",", ".").astype(float)

    return df
