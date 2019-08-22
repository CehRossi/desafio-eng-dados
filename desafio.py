import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # criando o contexto with Spark configuration
    conf = SparkConf().setAppName("Desafio Eng Dados")
    sc = SparkContext(conf=conf)

    # lendo os logs
    linhas = sc.textFile("./datasource/NASA_access_log*.gz")

    numero_de_linhas = linhas.count()
    print("===> total linhas: ", numero_de_linhas)