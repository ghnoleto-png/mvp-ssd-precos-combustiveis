# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Modelagem e carga (ETL em Spark)
# MAGIC **MVP de Sistemas de Suporte à Decisão (UnB) · Preços de combustíveis da ANP · Gabriel Noleto (211068010)**
# MAGIC
# MAGIC Entrada: os CSV da camada raw (`/Volumes/workspace/mvp_combustiveis/raw/csv`), gerados pelo notebook 01.
# MAGIC Saída: modelo em **esquema estrela** em tabelas Delta no Unity Catalog, tabela de exclusões com motivo,
# MAGIC controle de qualidade da carga e catálogo de dados.
# MAGIC
# MAGIC **Grão da fato:** um preço de venda de um produto, em um posto (CNPJ), em uma data de coleta.
# MAGIC
# MAGIC | Tabela | Tipo | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | `fato_preco` | fato | preço de venda (e de compra, quando houver) |
# MAGIC | `dim_tempo` | dimensão | calendário, com a semana da pesquisa ANP (domingo a sábado) |
# MAGIC | `dim_local` | dimensão | região, UF, município e se é capital |
# MAGIC | `dim_posto` | dimensão | CNPJ, razão social e endereço do posto |
# MAGIC | `dim_produto` | dimensão | produto, grupo (gasolina, etanol, diesel, GNV) e unidade |
# MAGIC | `dim_bandeira` | dimensão | bandeira e tipo (branca ou com bandeira) |
# MAGIC | `exclusoes_etl` | auxiliar | linhas descartadas, com a regra, o motivo e o conteúdo original |
# MAGIC | `qc_carga` | auxiliar | resultado das verificações de qualidade da carga |
# MAGIC | `catalogo_dados` | auxiliar | catálogo de dados de todos os atributos |
# MAGIC
# MAGIC Não há relação N:N (cada registro tem um único posto, produto, data e bandeira), por isso não há tabela-ponte.
# MAGIC A bandeira fica ligada à fato, e não ao posto, porque um posto pode trocar de bandeira ao longo do ano.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regras de transformação
# MAGIC As regras foram escritas a partir da amostra real impressa pelo notebook 01. As faixas plausíveis foram fixadas antes de olhar a distribuição dos preços.
# MAGIC
# MAGIC | Regra | O que faz | Se falhar |
# MAGIC |---|---|---|
# MAGIC | R0 | Linha totalmente vazia na fonte (só os 15 separadores, sem nenhum conteúdo) | vai para exclusões |
# MAGIC | R1 | Lê os CSV como texto (sem inferência de tipos), UTF-8, separador `;`, respeitando aspas: campos com `;` dentro vêm entre aspas, como `"QUADRA: 11; LOTE: 193;"`. Os nomes das colunas são definidos por posição, o que elimina o BOM do cabeçalho | linha com número de colunas diferente de 16 vai para exclusões |
# MAGIC | R2 | Texto: remove espaços e quebras nas pontas, passa para maiúsculas e trata texto vazio como nulo | — |
# MAGIC | R3 | Município sem acentos, para que a mesma cidade não vire duas chaves | — |
# MAGIC | R4 | CNPJ: mantém só os dígitos (unifica `08.775.979/0002-62` e `04431113000100`, os formatos dos dois semestres) | CNPJ sem exatamente 14 dígitos vai para exclusões |
# MAGIC | R5 | CEP: só dígitos, 8 posições | CEP inválido vira nulo |
# MAGIC | R6 | Número da rua: `SN`, `S/N`, `S N` e `S.N.` viram `S/N` | — |
# MAGIC | R7 | Data da coleta no formato `dd/MM/yyyy`, convertida com `try_to_timestamp` | data inválida vai para exclusões |
# MAGIC | R8 | A data precisa estar dentro do semestre do arquivo de origem | data fora do semestre vai para exclusões |
# MAGIC | R9 | Preços: vírgula vira ponto e o valor é convertido com `try_cast` para `DECIMAL(10,3)` | venda inválida vai para exclusões; compra inválida vira nulo |
# MAGIC | R10 | Faixa plausível do preço de venda: de R\$ 2,00 a R\$ 15,00 por litro ou m³. Compra: de R\$ 0,50 a R\$ 15,00 | venda fora da faixa vai para exclusões; compra fora vira nulo |
# MAGIC | R11 | Produto classificado em grupo (GASOLINA, ETANOL, DIESEL, GNV). A unidade precisa ser coerente: GNV em R\$/m³, os demais em R\$/litro | produto desconhecido ou unidade incoerente vai para exclusões |
# MAGIC | R12 | Campos obrigatórios: região, UF, município e produto | nulo vai para exclusões |
# MAGIC | R13 | Bandeira nula vira `NAO INFORMADA`. Tipo de bandeira: `BRANCA`, `COM BANDEIRA` ou `NAO INFORMADA` | — |
# MAGIC | R14 | Duplicata exata (mesmo CNPJ, produto, data e preço): mantém uma linha | as cópias vão para exclusões |
# MAGIC | R15 | Duplicata conflitante (mesmo CNPJ, produto e data, com preços diferentes): é ambígua, sai o grupo inteiro | o grupo vai para exclusões |
# MAGIC | R16 | `dim_posto`: dados cadastrais (razão social, endereço) da coleta mais recente do CNPJ | — |
# MAGIC | R17 | Chaves substitutas determinísticas (numeração pela chave natural ordenada); `sk_data` = AAAAMMDD | — |
# MAGIC | R18 | Semana da pesquisa ANP: de domingo a sábado (`semana_anp_inicio` = domingo da semana) | — |
# MAGIC
# MAGIC Quando uma linha falha em mais de uma regra, o motivo registrado é o da primeira que falhou, na ordem R0, R1, R12, R4, R7, R8, R9, R10, R11.

# COMMAND ----------

# Configuração
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType

CATALOG = "workspace"
SCHEMA = "mvp_combustiveis"
DB = f"{CATALOG}.{SCHEMA}"
PASTA_CSV = f"/Volumes/{CATALOG}/{SCHEMA}/raw/csv"
PASTA_CATALOGO = f"/Volumes/{CATALOG}/{SCHEMA}/raw/catalogo"
ARQUIVOS = ["ca-2025-01.csv", "ca-2025-02.csv"]
PRECO_MIN, PRECO_MAX = 2.0, 15.0      # R10, venda (R$/L ou R$/m³)
COMPRA_MIN, COMPRA_MAX = 0.5, 15.0    # R10, compra

def tab(nome):
    return f"{DB}.{nome}"

print("Destino:", DB)

# COMMAND ----------

# R1: leitura como texto, com colunas definidas por posição
COLUNAS_RAW = ["regiao_sigla", "uf", "municipio", "revenda", "cnpj_revenda", "nome_rua", "numero_rua",
               "complemento", "bairro", "cep", "produto", "data_coleta", "valor_venda", "valor_compra",
               "unidade_medida", "bandeira"]
schema_raw = StructType([StructField(c, StringType()) for c in COLUNAS_RAW] +
                        [StructField("_linha_malformada", StringType())])

partes = []
for nome in ARQUIVOS:
    partes.append(spark.read.format("csv")
                  .schema(schema_raw)
                  .option("header", "true")
                  .option("sep", ";")
                  .option("encoding", "UTF-8")
                  .option("quote", '"')                     # complementos com ';' vêm entre aspas, ex.: "QUADRA: 11; LOTE: 193;"
                  .option("escape", '"')
                  .option("mode", "PERMISSIVE")
                  .option("columnNameOfCorruptRecord", "_linha_malformada")
                  .load(f"{PASTA_CSV}/{nome}")
                  .withColumn("arquivo_origem", F.lit(nome)))
raw = partes[0].unionByName(partes[1])

# guarda o texto original de cada coluna, sem nenhuma alteração (vai para exclusoes_etl)
raw = raw.select("*", *[F.col(c).alias(f"orig_{c}") for c in COLUNAS_RAW])

# COMMAND ----------

# R2 a R13: padronização, tipagem e regra de exclusão
ESP = r"^[\s\u00a0]+|[\s\u00a0]+$"   # espaços, tabs, \r e espaço não separável nas pontas

def apara(c):
    return F.regexp_replace(F.col(c), ESP, "")

def limpa(c):
    """R2: apara, maiúsculas, vazio vira nulo."""
    return F.when(apara(c) == "", None).otherwise(F.upper(apara(c)))

ACENTOS, SEM_ACENTOS = "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ", "AAAAAEEEEIIIIOOOOOUUUUCN"

stg = (raw
    .withColumn("regiao_sigla", limpa("regiao_sigla"))
    .withColumn("uf", limpa("uf"))
    .withColumn("municipio", F.translate(limpa("municipio"), ACENTOS, SEM_ACENTOS))                    # R3
    .withColumn("razao_social", limpa("revenda"))
    .withColumn("cnpj_digitos", F.regexp_replace(F.coalesce(F.col("cnpj_revenda"), F.lit("")), r"\D", ""))
    .withColumn("cnpj", F.when(F.length("cnpj_digitos") == 14, F.col("cnpj_digitos")))               # R4
    .withColumn("nome_rua", limpa("nome_rua"))
    .withColumn("numero_rua", F.when(F.regexp_replace(limpa("numero_rua"), r"[\s/.]", "") == "SN", F.lit("S/N"))
                               .otherwise(limpa("numero_rua")))                                         # R6
    .withColumn("complemento", limpa("complemento"))
    .withColumn("bairro", limpa("bairro"))
    .withColumn("cep_digitos", F.regexp_replace(F.coalesce(F.col("cep"), F.lit("")), r"\D", ""))
    .withColumn("cep", F.when(F.length("cep_digitos") == 8, F.col("cep_digitos")))                     # R5
    .withColumn("produto", limpa("produto"))
    .withColumn("data_coleta", apara("data_coleta"))
    .withColumn("data", F.expr("to_date(try_to_timestamp(data_coleta, 'dd/MM/yyyy'))"))                # R7
    .withColumn("valor_venda", apara("valor_venda"))
    .withColumn("valor_compra", apara("valor_compra"))
    .withColumn("venda", F.expr("try_cast(replace(valor_venda, ',', '.') AS DECIMAL(10,3))"))          # R9
    .withColumn("compra", F.expr("try_cast(replace(valor_compra, ',', '.') AS DECIMAL(10,3))"))
    .withColumn("compra", F.when(F.col("compra").between(COMPRA_MIN, COMPRA_MAX), F.col("compra")))   # R10 (compra)
    .withColumn("unidade_medida", F.when(apara("unidade_medida") == "", None).otherwise(apara("unidade_medida")))
    .withColumn("unidade_norm", F.lower(F.regexp_replace(F.coalesce(F.col("unidade_medida"), F.lit("")), r"\s", "")))
    .withColumn("grupo_produto", F.expr("""CASE
        WHEN produto LIKE 'GASOLINA%' THEN 'GASOLINA'
        WHEN produto LIKE 'ETANOL%'   THEN 'ETANOL'
        WHEN produto LIKE 'DIESEL%'   THEN 'DIESEL'
        WHEN produto LIKE 'GNV%'      THEN 'GNV'
        ELSE 'OUTRO' END"""))                                                                           # R11
    .withColumn("unidade_ok", F.expr("""CASE
        WHEN grupo_produto = 'GNV' THEN unidade_norm IN ('r$/m³', 'r$/m3')
        ELSE unidade_norm = 'r$/litro' END"""))
    .withColumn("bandeira", F.coalesce(limpa("bandeira"), F.lit("NAO INFORMADA")))                     # R13
    .withColumn("ano_arq", F.expr("try_cast(substring(arquivo_origem, 4, 4) AS INT)"))   # ca-AAAA-SS.csv
    .withColumn("sem_arq", F.expr("try_cast(substring(arquivo_origem, 9, 2) AS INT)"))
    .withColumn("data_no_semestre", F.coalesce((F.year("data") == F.col("ano_arq")) &
                (F.when(F.month("data") <= 6, 1).otherwise(2) == F.col("sem_arq")), F.lit(False)))    # R8
)

conteudo_total = " || ".join(f"coalesce(regexp_replace(orig_{c}, '\\s', ''), '')" for c in COLUNAS_RAW)
stg = stg.withColumn("regra", F.expr(f"""CASE
        WHEN ({conteudo_total}) = '' THEN 'R0'
        WHEN _linha_malformada IS NOT NULL THEN 'R1'
        WHEN regiao_sigla IS NULL OR uf IS NULL OR municipio IS NULL OR produto IS NULL THEN 'R12'
        WHEN cnpj IS NULL THEN 'R4'
        WHEN data IS NULL THEN 'R7'
        WHEN NOT data_no_semestre THEN 'R8'
        WHEN venda IS NULL THEN 'R9'
        WHEN venda < {PRECO_MIN} OR venda > {PRECO_MAX} THEN 'R10'
        WHEN grupo_produto = 'OUTRO' OR NOT unidade_ok THEN 'R11'
        END"""))

COLS_STG = (["arquivo_origem", "regiao_sigla", "uf", "municipio", "razao_social", "cnpj", "nome_rua", "numero_rua",
             "complemento", "bairro", "cep", "produto", "grupo_produto", "unidade_medida", "data", "venda", "compra",
             "bandeira", "regra"] + [f"orig_{c}" for c in COLUNAS_RAW])

# grava a staging uma única vez (evita reprocessar os CSV em cada etapa seguinte)
stg.select(*COLS_STG).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("tmp_stg"))
stg = spark.table(tab("tmp_stg"))
print(f"Staging gravada: {stg.count():,} linhas lidas".replace(",", "."))

# COMMAND ----------

# R14 e R15: duplicatas
MOTIVOS = {
    "R0": "linha vazia na fonte (todos os 16 campos vazios)",
    "R1": "linha malformada (número de colunas diferente de 16)",
    "R12": "campo obrigatório nulo (região, UF, município ou produto)",
    "R4": "CNPJ ausente ou com número de dígitos diferente de 14",
    "R7": "data da coleta ausente ou fora do formato dd/MM/yyyy",
    "R8": "data da coleta fora do semestre do arquivo",
    "R9": "preço de venda ausente ou não numérico",
    "R10": f"preço de venda fora da faixa plausível [{PRECO_MIN:.2f}; {PRECO_MAX:.2f}]",
    "R11": "produto fora dos grupos conhecidos ou unidade incoerente com o produto",
    "R14": "duplicata exata (mesmo CNPJ, produto, data e preço): cópia removida",
    "R15": "duplicata conflitante (mesmo CNPJ, produto e data com preços diferentes)",
}
expr_motivo = F.expr("CASE " + " ".join(f"WHEN regra = '{k}' THEN '{v}'" for k, v in MOTIVOS.items()) + " END")

validas = stg.where("regra IS NULL")
w_grao = W.partitionBy("cnpj", "produto", "data")
validas = (validas
           .withColumn("n_precos", F.size(F.collect_set("venda").over(w_grao)))
           .withColumn("rn", F.row_number().over(w_grao.orderBy("arquivo_origem", "venda", "bandeira", "razao_social"))))
validas = validas.withColumn("regra", F.when(F.col("n_precos") > 1, "R15")
                                       .when(F.col("rn") > 1, "R14"))

limpas = validas.where("regra IS NULL").drop("n_precos", "rn", "regra")
limpas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("tmp_limpas"))
limpas = spark.table(tab("tmp_limpas"))

cols_excl = ["arquivo_origem", "regra"] + [f"orig_{c}" for c in COLUNAS_RAW]
exclusoes = (stg.where("regra IS NOT NULL").select(*cols_excl)
             .unionByName(validas.where("regra IS NOT NULL").select(*cols_excl))
             .withColumn("motivo", expr_motivo)
             .select("arquivo_origem", "regra", "motivo", *[f"orig_{c}" for c in COLUNAS_RAW]))
exclusoes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("exclusoes_etl"))

resumo_excl = (spark.table(tab("exclusoes_etl")).groupBy("regra", "motivo").count()
               .orderBy(F.desc("count")))
print(f"Linhas limpas: {limpas.count():,}".replace(",", "."))
display(resumo_excl)

# COMMAND ----------

# Dimensões
def numera(df, sk, *ordem):
    """R17: chave substituta determinística pela chave natural ordenada."""
    return df.withColumn(sk, F.row_number().over(W.orderBy(*ordem)).cast("int"))

# dim_tempo: calendário completo dos anos presentes nos dados (R18 para a semana ANP)
lim = limpas.agg(F.min("data").alias("dmin"), F.max("data").alias("dmax")).first()
ano_ini, ano_fim = lim["dmin"].year, lim["dmax"].year
dim_tempo = (spark.sql(f"SELECT explode(sequence(DATE'{ano_ini}-01-01', DATE'{ano_fim}-12-31', INTERVAL 1 DAY)) AS data")
    .select(
        F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
        "data",
        F.year("data").cast("int").alias("ano"),
        F.when(F.month("data") <= 6, 1).otherwise(2).cast("int").alias("semestre"),
        F.quarter("data").cast("int").alias("trimestre"),
        F.month("data").cast("int").alias("mes"),
        F.expr("""CASE month(data) WHEN 1 THEN 'JANEIRO' WHEN 2 THEN 'FEVEREIRO' WHEN 3 THEN 'MARCO'
                  WHEN 4 THEN 'ABRIL' WHEN 5 THEN 'MAIO' WHEN 6 THEN 'JUNHO' WHEN 7 THEN 'JULHO'
                  WHEN 8 THEN 'AGOSTO' WHEN 9 THEN 'SETEMBRO' WHEN 10 THEN 'OUTUBRO'
                  WHEN 11 THEN 'NOVEMBRO' ELSE 'DEZEMBRO' END""").alias("nome_mes"),
        F.dayofmonth("data").cast("int").alias("dia"),
        F.dayofweek("data").cast("int").alias("dia_semana"),
        F.expr("""CASE dayofweek(data) WHEN 1 THEN 'DOMINGO' WHEN 2 THEN 'SEGUNDA' WHEN 3 THEN 'TERCA'
                  WHEN 4 THEN 'QUARTA' WHEN 5 THEN 'QUINTA' WHEN 6 THEN 'SEXTA' ELSE 'SABADO' END""").alias("nome_dia_semana"),
        F.weekofyear("data").cast("int").alias("semana_iso"),
        F.expr("year(date_add(data, 3 - ((dayofweek(data) + 5) % 7)))").cast("int").alias("ano_iso"),
        F.expr("date_sub(data, dayofweek(data) - 1)").alias("semana_anp_inicio"),
        F.dayofweek("data").isin(1, 7).alias("eh_fim_de_semana"),
    ))

# dim_local: (UF, município) com indicação de capital
CAPITAIS = [("AC", "RIO BRANCO"), ("AL", "MACEIO"), ("AP", "MACAPA"), ("AM", "MANAUS"), ("BA", "SALVADOR"),
            ("CE", "FORTALEZA"), ("DF", "BRASILIA"), ("ES", "VITORIA"), ("GO", "GOIANIA"), ("MA", "SAO LUIS"),
            ("MT", "CUIABA"), ("MS", "CAMPO GRANDE"), ("MG", "BELO HORIZONTE"), ("PA", "BELEM"),
            ("PB", "JOAO PESSOA"), ("PR", "CURITIBA"), ("PE", "RECIFE"), ("PI", "TERESINA"),
            ("RJ", "RIO DE JANEIRO"), ("RN", "NATAL"), ("RS", "PORTO ALEGRE"), ("RO", "PORTO VELHO"),
            ("RR", "BOA VISTA"), ("SC", "FLORIANOPOLIS"), ("SP", "SAO PAULO"), ("SE", "ARACAJU"), ("TO", "PALMAS")]
df_capitais = spark.createDataFrame(CAPITAIS, "uf string, municipio string").withColumn("eh_capital", F.lit(True))
dim_local = (limpas.groupBy("uf", "municipio").agg(F.max("regiao_sigla").alias("regiao_sigla"))
    .join(df_capitais, ["uf", "municipio"], "left")
    .withColumn("eh_capital", F.coalesce("eh_capital", F.lit(False)))
    .withColumn("regiao_nome", F.expr("""CASE regiao_sigla WHEN 'N' THEN 'NORTE' WHEN 'NE' THEN 'NORDESTE'
        WHEN 'CO' THEN 'CENTRO-OESTE' WHEN 'SE' THEN 'SUDESTE' WHEN 'S' THEN 'SUL' END""")))
dim_local = numera(dim_local, "sk_local", "uf", "municipio").select(
    "sk_local", "regiao_sigla", "regiao_nome", "uf", "municipio", "eh_capital")

# dim_produto
dim_produto = numera(limpas.groupBy("produto").agg(F.max("grupo_produto").alias("grupo_produto"),
                                                    F.max("unidade_medida").alias("unidade_medida")),
                     "sk_produto", "produto").select("sk_produto", "produto", "grupo_produto", "unidade_medida")

# dim_bandeira
dim_bandeira = numera(limpas.select("bandeira").distinct()
    .withColumn("tipo_bandeira", F.expr("""CASE WHEN bandeira = 'BRANCA' THEN 'BRANCA'
        WHEN bandeira = 'NAO INFORMADA' THEN 'NAO INFORMADA' ELSE 'COM BANDEIRA' END""")),
    "sk_bandeira", "bandeira").select("sk_bandeira", "bandeira", "tipo_bandeira")

# dim_posto (R16: cadastro da coleta mais recente)
w_posto = W.partitionBy("cnpj").orderBy(F.desc("data"), F.desc("arquivo_origem"), "produto")
cadastro = (limpas.withColumn("rn", F.row_number().over(w_posto)).where("rn = 1")
            .select("cnpj", "razao_social", "nome_rua", "numero_rua", "complemento", "bairro", "cep"))
historico = limpas.groupBy("cnpj").agg(F.min("data").alias("primeira_coleta"), F.max("data").alias("ultima_coleta"),
                                       F.countDistinct("bandeira").cast("int").alias("qtd_bandeiras"))
dim_posto = numera(cadastro.join(historico, "cnpj"), "sk_posto", "cnpj").select(
    "sk_posto", "cnpj", "razao_social", "nome_rua", "numero_rua", "complemento", "bairro", "cep",
    "primeira_coleta", "ultima_coleta", "qtd_bandeiras")

for nome, df in [("dim_tempo", dim_tempo), ("dim_local", dim_local), ("dim_produto", dim_produto),
                 ("dim_bandeira", dim_bandeira), ("dim_posto", dim_posto)]:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab(nome))
    print(f"{nome:14} gravada: {spark.table(tab(nome)).count():>8,} linhas".replace(",", "."))

# COMMAND ----------

# Fato
fato = (limpas
    .join(spark.table(tab("dim_local")).select("sk_local", "uf", "municipio"), ["uf", "municipio"], "left")
    .join(spark.table(tab("dim_posto")).select("sk_posto", "cnpj"), "cnpj", "left")
    .join(spark.table(tab("dim_produto")).select("sk_produto", "produto"), "produto", "left")
    .join(spark.table(tab("dim_bandeira")).select("sk_bandeira", "bandeira"), "bandeira", "left")
    .select(F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"),
            "sk_local", "sk_posto", "sk_produto", "sk_bandeira",
            F.col("venda").cast("decimal(10,3)").alias("valor_venda"),
            F.col("compra").cast("decimal(10,3)").alias("valor_compra"),
            "arquivo_origem"))
fato.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("fato_preco"))
print(f"fato_preco     gravada: {spark.table(tab('fato_preco')).count():>8,} linhas".replace(",", "."))

# COMMAND ----------

# Controle de qualidade da carga
from pyspark.sql.types import LongType
qc = []
def check(nome, esperado, obtido):
    qc.append((nome, str(esperado), str(obtido), bool(esperado == obtido)))

f_ = spark.table(tab("fato_preco"))
e_ = spark.table(tab("exclusoes_etl"))
s_ = spark.table(tab("tmp_stg"))

# 1. contagem lida x manifesto do notebook 01
man = {r["arquivo"]: r["linhas_dados"] for r in
       spark.table(tab("manifesto_coleta")).where("formato = 'CSV'").select("arquivo", "linhas_dados").collect()}
lidas = {r["arquivo_origem"]: r["count"] for r in s_.groupBy("arquivo_origem").count().collect()}
for a in ARQUIVOS:
    check(f"linhas lidas = manifesto ({a})", man.get(a), lidas.get(a))

# 2. conciliação: lidas = carregadas + excluídas
n_lidas, n_fato, n_excl = s_.count(), f_.count(), e_.count()
check("conciliação: lidas = fato + exclusões", n_lidas, n_fato + n_excl)

# 3. grão único na fato
check("grão único na fato (sk_posto, sk_produto, sk_data)", n_fato,
      f_.select("sk_posto", "sk_produto", "sk_data").distinct().count())

# 4. chaves estrangeiras nulas e órfãs
for sk, dim in [("sk_data", "dim_tempo"), ("sk_local", "dim_local"), ("sk_posto", "dim_posto"),
                ("sk_produto", "dim_produto"), ("sk_bandeira", "dim_bandeira")]:
    check(f"{sk} nula na fato", 0, f_.where(F.col(sk).isNull()).count())
    check(f"{sk} órfã (sem par em {dim})", 0, f_.join(spark.table(tab(dim)), sk, "left_anti").count())

# 5. chaves únicas nas dimensões
for dim, sk, nat in [("dim_tempo", "sk_data", ["data"]), ("dim_local", "sk_local", ["uf", "municipio"]),
                     ("dim_posto", "sk_posto", ["cnpj"]), ("dim_produto", "sk_produto", ["produto"]),
                     ("dim_bandeira", "sk_bandeira", ["bandeira"])]:
    d = spark.table(tab(dim)); n = d.count()
    check(f"{sk} única em {dim}", n, d.select(sk).distinct().count())
    check(f"chave natural única em {dim} ({', '.join(nat)})", n, d.select(*nat).distinct().count())

# 6. preço dentro da faixa na fato
check("valor_venda fora da faixa na fato", 0,
      f_.where((F.col("valor_venda") < PRECO_MIN) | (F.col("valor_venda") > PRECO_MAX)).count())

df_qc = spark.createDataFrame(qc, "verificacao string, esperado string, obtido string, ok boolean")
df_qc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("qc_carga"))
n_ok = sum(1 for q in qc if q[3])
print(f"Verificações aprovadas: {n_ok} de {len(qc)}")
display(df_qc)

# COMMAND ----------

# Catálogo de dados (todos os atributos de todas as tabelas gravadas)
man_rows = spark.table(tab("manifesto_coleta")).where("formato = 'CSV'").select("arquivo", "data_coleta_utc").collect()
DATA_EXTRACAO = ", ".join(f"{r['arquivo']} em {r['data_coleta_utc'][:10]}" for r in man_rows)
FONTE = f"ANP, Série Histórica de Preços de Combustíveis, CSV semestrais de 2025 (extraídos: {DATA_EXTRACAO})"
ufs = ", ".join(sorted(u for u, _ in CAPITAIS))

def item(tabela, atributo, tipo, conceito, descricao, dominio, obrig, linhagem):
    return {"tabela": tabela, "atributo": atributo, "tipo": tipo, "tipo_conceitual": conceito,
            "descricao": descricao, "dominio": dominio, "obrigatorio": obrig, "linhagem": linhagem}

GERADA = "Gerada no notebook 02 (não vem da fonte)"
catalogo = [
    # fato_preco
    item("fato_preco", "sk_data", "int", "numérico (chave)", "Chave da data da coleta (FK para dim_tempo)",
         "AAAAMMDD, de 20250101 a 20251231", "Sim", f"{FONTE}: coluna 'Data da Coleta' (R7, R8, R17)"),
    item("fato_preco", "sk_local", "int", "numérico (chave)", "Chave do município (FK para dim_local)",
         "inteiro >= 1", "Sim", f"{FONTE}: 'Estado - Sigla' + 'Municipio' (R2, R3, R17)"),
    item("fato_preco", "sk_posto", "int", "numérico (chave)", "Chave do posto (FK para dim_posto)",
         "inteiro >= 1", "Sim", f"{FONTE}: 'CNPJ da Revenda' (R4, R17)"),
    item("fato_preco", "sk_produto", "int", "numérico (chave)", "Chave do produto (FK para dim_produto)",
         "inteiro >= 1", "Sim", f"{FONTE}: 'Produto' (R2, R11, R17)"),
    item("fato_preco", "sk_bandeira", "int", "numérico (chave)", "Chave da bandeira do posto na data (FK para dim_bandeira)",
         "inteiro >= 1", "Sim", f"{FONTE}: 'Bandeira' (R2, R13, R17)"),
    item("fato_preco", "valor_venda", "decimal(10,3)", "numérico", "Preço de venda ao consumidor (R$ por litro; R$ por m³ no GNV)",
         f"de {PRECO_MIN:.2f} a {PRECO_MAX:.2f}", "Sim", f"{FONTE}: 'Valor de Venda' (R9, R10)"),
    item("fato_preco", "valor_compra", "decimal(10,3)", "numérico", "Preço de compra pago pelo posto, quando a pesquisa informa",
         f"de {COMPRA_MIN:.2f} a {COMPRA_MAX:.2f}", "Não (nulo = não informado na pesquisa ou fora da faixa)",
         f"{FONTE}: 'Valor de Compra' (R9, R10)"),
    item("fato_preco", "arquivo_origem", "string", "categórico", "Arquivo semestral de origem da linha",
         "ca-2025-01.csv, ca-2025-02.csv", "Sim", "Nome do arquivo baixado no notebook 01"),
    # dim_tempo
    item("dim_tempo", "sk_data", "int", "numérico (chave)", "Chave da data (AAAAMMDD)", "de 20250101 a 20251231", "Sim", f"{GERADA} (R17)"),
    item("dim_tempo", "data", "date", "data", "Data do calendário", "de 2025-01-01 a 2025-12-31", "Sim", GERADA),
    item("dim_tempo", "ano", "int", "numérico", "Ano", "2025", "Sim", GERADA),
    item("dim_tempo", "semestre", "int", "categórico", "Semestre do ano", "1, 2", "Sim", GERADA),
    item("dim_tempo", "trimestre", "int", "categórico", "Trimestre do ano", "1 a 4", "Sim", GERADA),
    item("dim_tempo", "mes", "int", "numérico", "Mês", "1 a 12", "Sim", GERADA),
    item("dim_tempo", "nome_mes", "string", "categórico", "Nome do mês (sem acento)", "JANEIRO a DEZEMBRO", "Sim", GERADA),
    item("dim_tempo", "dia", "int", "numérico", "Dia do mês", "1 a 31", "Sim", GERADA),
    item("dim_tempo", "dia_semana", "int", "categórico", "Dia da semana (1 = domingo)", "1 a 7", "Sim", GERADA),
    item("dim_tempo", "nome_dia_semana", "string", "categórico", "Nome do dia da semana (sem acento)",
         "DOMINGO, SEGUNDA, TERCA, QUARTA, QUINTA, SEXTA, SABADO", "Sim", GERADA),
    item("dim_tempo", "semana_iso", "int", "numérico", "Semana do ano no padrão ISO 8601 (segunda a domingo)", "1 a 53", "Sim", GERADA),
    item("dim_tempo", "ano_iso", "int", "numérico", "Ano ao qual a semana ISO pertence (29 a 31/12/2025 caem na semana 1 de 2026)",
         "2025, 2026", "Sim", GERADA),
    item("dim_tempo", "semana_anp_inicio", "date", "data", "Domingo que inicia a semana da pesquisa ANP (domingo a sábado)",
         "domingos de 2024-12-29 a 2025-12-28", "Sim", f"{GERADA} (R18)"),
    item("dim_tempo", "eh_fim_de_semana", "boolean", "booleano", "Sábado ou domingo", "true, false", "Sim", GERADA),
    # dim_local
    item("dim_local", "sk_local", "int", "numérico (chave)", "Chave do município", "inteiro >= 1", "Sim", f"{GERADA} (R17)"),
    item("dim_local", "regiao_sigla", "string", "categórico", "Sigla da região", "N, NE, CO, SE, S", "Sim", f"{FONTE}: 'Regiao - Sigla' (R2)"),
    item("dim_local", "regiao_nome", "string", "categórico", "Nome da região", "NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL",
         "Sim", f"Derivada de regiao_sigla no notebook 02"),
    item("dim_local", "uf", "string", "categórico", "Sigla da unidade da federação", f"27 siglas: {ufs}", "Sim",
         f"{FONTE}: 'Estado - Sigla' (R2)"),
    item("dim_local", "municipio", "string", "texto", "Nome do município, em maiúsculas e sem acento",
         "municípios pesquisados pela ANP", "Sim", f"{FONTE}: 'Municipio' (R2, R3)"),
    item("dim_local", "eh_capital", "boolean", "booleano", "Município é capital da UF", "true (27 capitais), false",
         "Sim", "Lista fixa das 27 capitais no notebook 02"),
    # dim_posto
    item("dim_posto", "sk_posto", "int", "numérico (chave)", "Chave do posto", "inteiro >= 1", "Sim", f"{GERADA} (R17)"),
    item("dim_posto", "cnpj", "string", "texto (identificador)", "CNPJ do posto revendedor, só dígitos", "14 dígitos", "Sim",
         f"{FONTE}: 'CNPJ da Revenda' (R4)"),
    item("dim_posto", "razao_social", "string", "texto", "Razão social do posto", "texto livre em maiúsculas",
         "Não (nulo = não informado)", f"{FONTE}: 'Revenda' (R2, R16)"),
    item("dim_posto", "nome_rua", "string", "texto", "Logradouro", "texto livre em maiúsculas", "Não (nulo = não informado)",
         f"{FONTE}: 'Nome da Rua' (R2, R16)"),
    item("dim_posto", "numero_rua", "string", "texto", "Número do endereço", "texto; sem número = S/N", "Não (nulo = não informado)",
         f"{FONTE}: 'Numero Rua' (R2, R6, R16)"),
    item("dim_posto", "complemento", "string", "texto", "Complemento do endereço", "texto livre em maiúsculas",
         "Não (nulo = sem complemento)", f"{FONTE}: 'Complemento' (R2, R16)"),
    item("dim_posto", "bairro", "string", "texto", "Bairro", "texto livre em maiúsculas", "Não (nulo = não informado)",
         f"{FONTE}: 'Bairro' (R2, R16)"),
    item("dim_posto", "cep", "string", "texto (identificador)", "CEP, só dígitos", "8 dígitos", "Não (nulo = ausente ou inválido)",
         f"{FONTE}: 'Cep' (R5, R16)"),
    item("dim_posto", "primeira_coleta", "date", "data", "Primeira data em que o posto aparece no período",
         "de 2025-01-01 a 2025-12-31", "Sim", "Calculada no notebook 02"),
    item("dim_posto", "ultima_coleta", "date", "data", "Última data em que o posto aparece no período",
         "de 2025-01-01 a 2025-12-31", "Sim", "Calculada no notebook 02"),
    item("dim_posto", "qtd_bandeiras", "int", "numérico", "Quantas bandeiras diferentes o posto teve no período",
         "inteiro >= 1", "Sim", "Calculada no notebook 02"),
    # dim_produto
    item("dim_produto", "sk_produto", "int", "numérico (chave)", "Chave do produto", "inteiro >= 1", "Sim", f"{GERADA} (R17)"),
    item("dim_produto", "produto", "string", "categórico", "Produto conforme a ANP",
         "nomes da ANP iniciados por GASOLINA, ETANOL, DIESEL ou GNV", "Sim", f"{FONTE}: 'Produto' (R2, R11)"),
    item("dim_produto", "grupo_produto", "string", "categórico", "Grupo do produto", "GASOLINA, ETANOL, DIESEL, GNV",
         "Sim", "Derivado de produto no notebook 02 (R11)"),
    item("dim_produto", "unidade_medida", "string", "categórico", "Unidade do preço", "R$ / litro, R$ / m³", "Sim",
         f"{FONTE}: 'Unidade de Medida' (R11)"),
    # dim_bandeira
    item("dim_bandeira", "sk_bandeira", "int", "numérico (chave)", "Chave da bandeira", "inteiro >= 1", "Sim", f"{GERADA} (R17)"),
    item("dim_bandeira", "bandeira", "string", "categórico", "Distribuidora cuja marca o posto exibe; BRANCA = sem bandeira",
         "nomes da ANP (ex.: BRANCA, IPIRANGA, RAIZEN, VIBRA) e NAO INFORMADA", "Sim", f"{FONTE}: 'Bandeira' (R2, R13)"),
    item("dim_bandeira", "tipo_bandeira", "string", "categórico", "Tipo de bandeira", "BRANCA, COM BANDEIRA, NAO INFORMADA",
         "Sim", "Derivado de bandeira no notebook 02 (R13)"),
    # exclusoes_etl
    item("exclusoes_etl", "arquivo_origem", "string", "categórico", "Arquivo de origem da linha excluída",
         "ca-2025-01.csv, ca-2025-02.csv", "Sim", "Nome do arquivo baixado no notebook 01"),
    item("exclusoes_etl", "regra", "string", "categórico", "Regra que causou a exclusão",
         "R0, R1, R4, R7, R8, R9, R10, R11, R12, R14, R15", "Sim", GERADA),
    item("exclusoes_etl", "motivo", "string", "texto", "Descrição do motivo da exclusão", "texto fixo por regra", "Sim", GERADA),
]
for c, desc in zip(COLUNAS_RAW, ["Regiao - Sigla", "Estado - Sigla", "Municipio", "Revenda", "CNPJ da Revenda",
                                 "Nome da Rua", "Numero Rua", "Complemento", "Bairro", "Cep", "Produto",
                                 "Data da Coleta", "Valor de Venda", "Valor de Compra", "Unidade de Medida", "Bandeira"]):
    catalogo.append(item("exclusoes_etl", f"orig_{c}", "string", "texto",
                         f"Conteúdo original da coluna '{desc}', sem nenhuma alteração", "texto como veio da fonte",
                         "Não (nulo = vazio na fonte)", f"{FONTE}: '{desc}' (R1)"))

CAMPOS_CAT = ["tabela", "atributo", "tipo", "tipo_conceitual", "descricao", "dominio", "obrigatorio", "linhagem"]
df_cat = spark.createDataFrame([tuple([i + 1] + [c[k] for k in CAMPOS_CAT]) for i, c in enumerate(catalogo)],
                               "ordem int, " + ", ".join(f"{k} string" for k in CAMPOS_CAT))
df_cat.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("catalogo_dados"))

# cópia em CSV no Volume (vai para a pasta /catalogo do repositório)
import os, pandas as pd
os.makedirs(PASTA_CATALOGO, exist_ok=True)
pd.DataFrame(catalogo).to_csv(f"{PASTA_CATALOGO}/catalogo_dados.csv", index=False, encoding="utf-8-sig")
print(f"Catálogo: {len(catalogo)} atributos gravados em {tab('catalogo_dados')} e em {PASTA_CATALOGO}/catalogo_dados.csv")

# COMMAND ----------

# Conferência: o catálogo bate com o schema real das tabelas?
TABELAS_MODELO = ["fato_preco", "dim_tempo", "dim_local", "dim_posto", "dim_produto", "dim_bandeira", "exclusoes_etl"]
real = {(t, f.name, f.dataType.simpleString()) for t in TABELAS_MODELO for f in spark.table(tab(t)).schema.fields}
cat = {(c["tabela"], c["atributo"], c["tipo"]) for c in catalogo}
so_no_real, so_no_catalogo = sorted(real - cat), sorted(cat - real)
print(f"Atributos nas tabelas: {len(real)} | no catálogo: {len(cat)}")
print("Faltando no catálogo:", so_no_real if so_no_real else "nenhum")
print("No catálogo mas não nas tabelas (ou com tipo diferente):", so_no_catalogo if so_no_catalogo else "nenhum")
print("RESULTADO:", "CATÁLOGO CONFERE COM O SCHEMA REAL" if not so_no_real and not so_no_catalogo else "DIVERGÊNCIA, ver acima")

# Descrições do catálogo viram comentários das colunas no Unity Catalog
def esc(s):
    return s.replace("'", "\\'")
COMENT_TAB = {
    "fato_preco": "Fato: preço de venda por posto, produto e data de coleta (ANP, 2025)",
    "dim_tempo": "Dimensão calendário com semana da pesquisa ANP",
    "dim_local": "Dimensão município, UF e região",
    "dim_posto": "Dimensão posto revendedor (CNPJ)",
    "dim_produto": "Dimensão produto e grupo",
    "dim_bandeira": "Dimensão bandeira do posto",
    "exclusoes_etl": "Linhas excluídas no ETL com regra e motivo",
}
for t, ctxt in COMENT_TAB.items():
    spark.sql(f"COMMENT ON TABLE {tab(t)} IS '{esc(ctxt)}'")
for c in catalogo:
    spark.sql(f"ALTER TABLE {tab(c['tabela'])} ALTER COLUMN {c['atributo']} COMMENT '{esc(c['descricao'])}'")
print("Comentários aplicados às tabelas e colunas no Unity Catalog")

# COMMAND ----------

# Limpeza das tabelas temporárias
for t in ["tmp_stg", "tmp_limpas"]:
    spark.sql(f"DROP TABLE IF EXISTS {tab(t)}")
print("Tabelas temporárias removidas")

# COMMAND ----------

# RESUMO 1 de 3: contagens
print("RESUMO 1/3: CONTAGENS DA CARGA")
print(f"Linhas lidas dos CSV: {n_lidas:,}".replace(",", "."))
print(f"Linhas carregadas na fato: {n_fato:,}".replace(",", "."))
print(f"Linhas excluídas: {n_excl:,} ({100 * n_excl / n_lidas:.2f}%)".replace(",", "."))
for t in ["fato_preco", "dim_tempo", "dim_local", "dim_posto", "dim_produto", "dim_bandeira", "exclusoes_etl"]:
    print(f"- {t}: {spark.table(tab(t)).count():,} linhas".replace(",", "."))

# COMMAND ----------

# RESUMO 2 de 3: exclusões e qualidade da carga
print("RESUMO 2/3: EXCLUSÕES POR REGRA")
for r in spark.table(tab("exclusoes_etl")).groupBy("regra", "motivo").count().orderBy(F.desc("count")).collect():
    print(f"- {r['regra']}: {r['count']:,} | {r['motivo']}".replace(",", "."))
print("\nEXCLUSÕES POR UF (exceto linhas vazias):")
for r in (spark.table(tab("exclusoes_etl")).where("regra <> 'R0'").groupBy("orig_uf").count()
          .orderBy(F.desc("count")).limit(10).collect()):
    print(f"- {r['orig_uf']}: {r['count']:,}".replace(",", "."))
print("\nCONTROLE DE QUALIDADE DA CARGA")
for q in qc:
    print(f"- [{'OK' if q[3] else 'FALHA'}] {q[0]} | esperado {q[1]} | obtido {q[2]}")

# COMMAND ----------

# RESUMO 3 de 3: conteúdo das dimensões pequenas (domínios observados)
print("RESUMO 3/3: DOMÍNIOS OBSERVADOS")
print("Produtos:")
for r in spark.table(tab("dim_produto")).orderBy("produto").collect():
    n = f_.where(F.col("sk_produto") == r["sk_produto"]).count()
    print(f"- {r['produto']} | grupo {r['grupo_produto']} | {r['unidade_medida']} | {n:,} preços".replace(",", "."))
print("\nBandeiras (top 15 por número de preços):")
top = (f_.groupBy("sk_bandeira").count().join(spark.table(tab("dim_bandeira")), "sk_bandeira")
       .orderBy(F.desc("count")).limit(15).collect())
for r in top:
    print(f"- {r['bandeira']} ({r['tipo_bandeira']}): {r['count']:,}".replace(",", "."))
print(f"\nTotal de bandeiras distintas: {spark.table(tab('dim_bandeira')).count()}")
dl = spark.table(tab("dim_local"))
print(f"UFs: {dl.select('uf').distinct().count()} | municípios: {dl.count()} | capitais encontradas: {dl.where('eh_capital').count()} de 27")
faltando = sorted(set(CAPITAIS) - {(r['uf'], r['municipio']) for r in dl.where('eh_capital').collect()})
print("Capitais não encontradas nos dados:", faltando if faltando else "nenhuma")
dp = spark.table(tab("dim_posto"))
print(f"Postos (CNPJ): {dp.count():,} | postos que trocaram de bandeira no período: {dp.where('qtd_bandeiras > 1').count():,}".replace(",", "."))
