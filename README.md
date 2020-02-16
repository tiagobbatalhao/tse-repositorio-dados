# Explorando os dados eleitorais do Brasil

Uma boa fonte de dados sobre o perfil das regiões brasileiras está disponível no [Repositório de Dados Eleitorais do TSE](http://www.tse.jus.br/eleicoes/estatisticas/repositorio-de-dados-eleitorais-1/repositorio-de-dados-eleitorais). Além das informações sobre eleições, com resultados e candidatos, há também informações sobre o perfil demográfico, como idade, gênero e grau de escolaridade de cada zona e seção eleitoral.


## Baixando os dados do Repositório

O repositório contém dados sobre várias eleições passadas, permitindo fazer comparações históricas. No entanto, os arquivos estão salvos em formatos diferentes. Para facilitar a com, criei um script para converter todos os arquivos para um formato padrão.

Para rodar a partir da linha de comando, digite

```
python tse_download_repositorio.py --dados <dados> --anos <anos>
```

O parâmetro dados é uma string com os dados desejados, separados por vírgula. As possibilidades são 

* `candidatos`: dados pessoais sobre candidatos nas eleições.
* `demografia_zona`: dados demográficos no nível de zona eleitoral.
* `demografia_secao`: dados demográficos no nível de seção eleitoral.
* `demografia`: todos os dados demográficos.
* `votos_zona`: votação no nível de zona eleitoral.
* `votos_secao`: votação no nível de seção eleitoral.
* `votos_detalhe`: informação sobre comparecimento e abstenção.
* `votos`: todos os dados de votação.
* `tudo`: todos os dados anteriores.

Todos os dados serão salvos na pasta `~/localdatalake/tse_refined/`. Futuramente, pretendo tornar esse script mais user-friendly para outros usuários.

