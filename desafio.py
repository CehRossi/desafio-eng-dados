import sys
from utils import Utils
from operator import add
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # criando o contexto with Spark configuration
    conf = SparkConf().setAppName("Desafio Eng Dados")
    sc = SparkContext(conf=conf)

    # lendo os arquivos de log
    logs = sc.textFile("./in/NASA_access_log*.gz")

    # trato os dados para começar as análises
    acessos = logs.map(lambda linha: Utils.linha_para_array(linha))
    #acessos.saveAsTextFile("out/acessos")
    
    # Valido a qtde de logs invalidos
    logs_invalidos =  acessos \
                         .filter(lambda a: len(a[4]) > 0)
    # Apenas uma linha com erro, optei por desconsidera-la    
    print("===> total logs: ", logs.count())
    print("===> total logs inválidos: ", logs_invalidos.count())
    
    ############# RESOLUÇÕES ############# 

    # 1) Listando a quantidade de hosts distintos
    numero_de_hosts = acessos \
                        .map(lambda a: a[0]) \
                        .distinct()
    print("===> DESAFIO 1: Total de hosts distintos: ", numero_de_hosts.count())
    # Achou 137.979

    acessos.saveAsTextFile("out/acessos")

    # 2) Listar o total de erros 404
    logs_404 = acessos \
                .filter(lambda a: a[2] == 404)
    print("===> DESAFIO 2: Total de erros 404: ", logs_404.count())
    # Achou 20.901

    # 3) Listar o top 5 de hosts que geraram 404
    top_5_hosts_404 = logs_404 \
                        .map(lambda a: (a[0],1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda a: a[1], False) \
                        .take(5)
    print("===> DESAFIO 3: Top 5 hosts com 404: ", top_5_hosts_404)

    # 4) Listar a qdte de erro 404 por dia
    erro_404_por_dia = logs_404 \
                         .map(lambda a: (Utils.date_to_string(a[1]),1)) \
                         .reduceByKey(lambda a, b: a + b)
    erro_404_por_dia.saveAsTextFile("out/erro_404_por_dia")

    # 5) Calcular o total de bytes retornados 
    total_bytes_retornados = acessos \
                                .map(lambda a: a[3]) \
                                .reduce(add)
    print("===> DESAFIO 5: Total de bytes trafegados: ", total_bytes_retornados)
    # Achou 65524314915 bytes (65,52Gb)