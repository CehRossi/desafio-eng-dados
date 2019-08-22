import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # criando o contexto with Spark configuration
    conf = SparkConf().setAppName("Desafio Eng Dados")
    sc = SparkContext(conf=conf)

    # lendo os logs
    linhas = sc.textFile("/datasource/NASA_access_log*.gz")

    numero_de_linhas = linhas.map(lambda linha : linha.split("\n"))

    # Transformandoo RDD em uma lista
    lista_de_linhas = numero_de_linhas.collect()

    # printa tudo
    for linha in lista_de_linhas:
        i += 1
        print("==> Linha", i, linhas, sep=" ")