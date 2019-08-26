# Desafio de Engenheiro de Dados

### Respostas técnicas sobre Spark:

1. Qual o objetivo do comando **cache** em Spark?<br>
> R: *O objetivo do comando **cache** no Spark é manter um RDD em memória visando melhorar a performance de um processo, onde por exemplo, há um RDD sendo reutilizado diversas vezes. Quando fazemos o cache deste RDD, o Spark não reavaliará este RDD quando alguma uma proxima ação for realizada nele.*
<br><br>

2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?<br>
>R: *Porque, enquanto o **Spark** processa dados em memória, o **MapReduce** precisa ler e gravar estes dados em um disco para processa-los.*
<br><br>

3. Qual é a função do **SparkContext**?<br>
>R: *A função do **SparkContext** é definir os parâmetros iniciais de uma aplicação Spark, como por exemplo, o nome do processo, cluster onde encontra-se o servidor do Spark, entre outros*
<br><br>

4. Explique com suas palavras o que é **Resilient Distributed Datasets** (RDD). <br>
> R: *É um conjunto de dados, que armazena qualquer tipo de dado, sendo o principal input para o Spark e fonte essencial para análises através de transformações e ações.*
<br><br>

5. **GroupByKey** é menos eficiente que **reduceByKey** em grandes dataset. Por quê?<br>
> R: *Quando um o **groupByKey** é executado ele organiza os dados a fim de formar os grupos de chave e valor (o que pode ser um processo demorado em quand aplicado em um grupo grande de dados), enquanto o **reduceByKey** aplica uma função de reduce e só organiza os dados para o conjunto que atende a função passada, resultando numa performance significativamente melhor.*
<br><br>

6. Explique o que o código Scala abaixo faz.<br>

        val textFile = sc.textFile("hdfs://...") <br>
        
Atribui à variável **textFile** um RDD construido através de um diretório HDFS<br>

        val counts = textFile.flatMap(line => line.split(" "))
                               .map(word => (word, 1))
                               .reduceByKey(_ + _)<br>
                               
Atribui à variável **counts** um novo RDD aplicando as seguintes tarefas: <br>
1. Um split por espaço em branco no RDD **textFile**
2. Defini uma tupla onde o primeiro valor é atual palavra e o segundo um valor fixo (1)
3. Aplica um reduce para computar o total de palvras usando o primeiro valor da tupla (a palavra) como chave                               
    
        counts.saveAsTextFile("hdfs://...")
        
Grava o RDD **counts** como arquivo texto no HDFS
<br><br>

### Considerações
Gostaria de agradecer a oportunidade de participar deste desafio em Spark. Foi meu primeiro contato com a ferramenta e, apesar de não conseguir entregar o resultado das análises de uma maneira mais elegante e amigável, este desafio agregou muito conhecimento em um espaço curto de tempo e reforçou muito minha auto-estima e meu gosto por TI. Estou bem satisfeito pelo resultado que eu obtive e feliz por ter concluído este desafio.
Até mais!!!