**- Qual o ​objetivo do comando cache em Spark?**

O comando cache tem como objetivo de carregar os dados do dataset em memória, de maneira a melhorar a performance da aplicação. É muito útil quando o dataset é utilizado várias vezes, pois o Spark trabalha com carregamento do tipo lazy load, ou seja, o dataset só será lido quando uma função for executada sobre ele.

**- O mesmo código implementado em Spark é normalmente mais rápido que a Implementação equivalente Em MapReduce. Por quê?**

Cada job do Map Reduce guarda os resultados do processamento em disco. Quando há vários Jobs em sequência para serem executados, cada job irá ler o dado, processar, guardar em disco, sem reutilização dos dados em memória. Já o Spark, é possível guardar o resultado do processamento em memória, e utilizar os resultados intermediários em memória em outros Jobs.

**- Qual é a função do SparkContext?**

O SparkContext  é um cliente de execução do spark e é a partir dele que os serviços e a coneção com o ambiente do spark é realizado. A partir dele que podemos criar RDDs, acumuladores, realizar broadcast de variáveis, executar Jobs, etc.

**- Explique com suas palavras o que é Resilient Distributed Datasets (RDD).**

Resilient Distributed Dataset é a estrutura fundamental do Spark. Eles são coleções imutáveis que podem realizar processamento em diferentes nodes do cluster. São tolerantes a falhas, podem ser particionados, sua execução é em memória e são carregados de o mais tarde possível (lazy evaluation).

**- GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**

Devido ao volume de dados trafegado. Quando é realizado o comando reduceByKey, o spark entende que poderá agrupar em cada nó de execução e trafegar o resultado. Já o groupByKey, não ocorre esse processamento parcial em cada nó, desta maneira, muito mais dados são trafegados.

**- Explique o que o código Scala abaixo faz.**

Linha 1: O arquivo de texto é lido.

Linha 2: Cada linha é quebrada em uma sequência de palavras e transformadas em uma coleção de palavras.

Linha 3: Cada palavra é mapeada para um conjunto de chave valor.

Linha 4: Ocorre uma agregação das palavras, resultando no número na palavra e quantidade em que ela ocorreu.

Linha 5: Salva o resultado no formato texto.

