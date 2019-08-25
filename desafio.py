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
    
    # numero_de_erros =  acessos \
    #                      .filter(lambda a: len(a[4]) > 0)
    # # Apenas uma linha com erro, avaliar o que fazer neste caso                        
    # print("===> total erros: ", numero_de_erros.count())

    # # Listando a quantidade de hosts distintos
    # numero_de_hosts = acessos \
    #                     .map(lambda a: a[0]) \
    #                     .distinct() 
    # print("===> total hosts distintos: ", numero_de_hosts.count())
    # # Achou 137979

    # # Listando o total de erros 404
    # total_de_404 = acessos \
    #                 .map(lambda a: a[2]) \
    #                 .filter(lambda a: a == "404") 
    # print("===> total erro 404: ", total_de_404.count())
    # # Achou 20901

    # Listar o top 5 de hosts que geraram 404
    top_5_hosts_404 = acessos \
                        .filter(lambda a: a[2] == "404") \
                        .map(lambda a: (a[0],1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda a: a[1], False) \
                        .take(5)
    print(">>>>>> Top 5 hosts com 404: ", top_5_hosts_404)

    # Listar a qdtde de erro 404 por dia
    
    # Calcular o total de bytes retornados 