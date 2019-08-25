import sys
from utils import Utils

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # criando o contexto with Spark configuration
    conf = SparkConf().setAppName("Desafio Eng Dados")
    sc = SparkContext(conf=conf)

    # lendo os logs
    logs = sc.textFile("./datasource/NASA_access_log*.gz")

    total_de_logs = logs.count()
    print("===> total linhas: ", total_de_logs)

    acessos = logs.map(lambda linha: Utils.linha_para_array(linha))
    #acessos.saveAsTextFile("out/acessos")
    
    numero_de_erros =  acessos \
                         .filter(lambda a: len(a[4]) > 0)
    # Apenas uma linha com erro, avaliar o que fazer neste caso                        
    print("===> total erros: ", numero_de_erros.count())

    # # Listando a quantidade de hosts distintos
    # numero_de_hosts = acessos \
    #                     .map(lambda a: a["host"]) \
    #                     .distinct() 
    # print("===> total hosts distintos: ", numero_de_hosts.count())
    # # Achou 137979

    # # Listando o total de erros 404
    # total_de_404 = acessos \
    #                 .map(lambda a: a["http_code"]) \
    #                 .filter(lambda a: a == "404") 
    # print("===> total erro 404: ", total_de_404.count())
    # # Achou 20901