import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    def refina_log(linha):
        dados = linha.split(" ")
        
        host = dados[0]

        try:
            bytes_enviados = dados[::-1][0]
            http_code = dados[::-1][1]
        except:
            bytes_enviados = "xxxxxxx"
            http_code = "xxxxxxx"

        return {
            "host": host,
            "bytes_enviados": bytes_enviados,
            "http_code": http_code
        }

    # criando o contexto with Spark configuration
    conf = SparkConf().setAppName("Desafio Eng Dados")
    sc = SparkContext(conf=conf)

    # lendo os logs
    logs = sc.textFile("./datasource/NASA_access_log*.gz")

    total_de_logs = logs.count()
    print("===> total linhas: ", total_de_logs)

    acessos = logs.map(lambda l: refina_log(l))
    numero_de_erros =  acessos \
                        .filter(lambda a: a["http_code"] == "xxxxxxx")

    # Apenas uma linha com erro, avaliar o que fazer neste caso                        
    print("===> total erros: ", numero_de_erros.count())