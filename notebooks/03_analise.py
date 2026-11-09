# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Análise
# MAGIC **MVP de Sistemas de Suporte à Decisão (UnB) · Preços de combustíveis da ANP · Gabriel Noleto (211068010)**
# MAGIC
# MAGIC Parte A: qualidade dos dados por atributo (nulos, distintos, mínimo, máximo, valores fora do domínio declarado no catálogo).
# MAGIC Parte B: qualidade por dimensão (completude, unicidade, consistência, conformidade, acurácia, atualidade).
# MAGIC Parte C: resposta às perguntas de negócio, definidas antes de olhar os dados:
# MAGIC
# MAGIC | | Pergunta | Critério fixado no Objetivo |
# MAGIC |---|---|---|
# MAGIC | P1 | Em quais UFs, e em quantas semanas de 2025, o etanol compensou frente à gasolina comum? | razão entre as medianas semanais (etanol / gasolina) ≤ 0,70; H1 se confirma se menos da metade das UFs compensar na maioria das semanas |
# MAGIC | P2 | No mesmo município, postos com bandeira cobram mais pela gasolina comum do que os de bandeira branca? | diferença entre medianas por município; Wilcoxon pareado unilateral, 5%; H2 se confirma se a diferença for positiva e significativa |
# MAGIC | P3 | Qual a diferença de preço da gasolina entre os postos mais baratos e os mais caros de um mesmo município, e onde fica Brasília entre as capitais? | amplitude P90 − P10 em % da mediana, municípios com ≥ 10 postos; relevante se ≥ 5%; H3 se confirma se o município típico tiver amplitude ≥ 5% |
# MAGIC
# MAGIC Medidas robustas (mediana e percentis) são usadas em todo o notebook, porque preços têm valores extremos.
# MAGIC As figuras são salvas em PNG no Volume (`/Volumes/workspace/mvp_combustiveis/raw/figuras`).

# COMMAND ----------

# Configuração
import os, shutil
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
from pyspark.sql import functions as F

try:
    from scipy import stats
    TEM_SCIPY = True
except Exception:
    TEM_SCIPY = False
    print("Aviso: scipy indisponível; testes estatísticos não serão calculados")

CATALOG, SCHEMA = "workspace", "mvp_combustiveis"
DB = f"{CATALOG}.{SCHEMA}"
PASTA_FIG = f"/Volumes/{CATALOG}/{SCHEMA}/raw/figuras"
os.makedirs(PASTA_FIG, exist_ok=True)

def tab(nome):
    return f"{DB}.{nome}"

# parâmetros fixados no Objetivo (antes de olhar os dados)
LIMIAR_ETANOL = 0.70      # P1
MIN_PRECOS_SEMANA = 5     # P1: preços mínimos de cada produto por UF e semana para calcular a mediana
MIN_PRECOS_TIPO = 10      # P2: preços mínimos de cada tipo de bandeira por município
MIN_POSTOS = 10           # P3: postos mínimos por município e semana
LIMIAR_DISPERSAO = 0.05   # P3
ALFA = 0.05
TANQUE_L = 50             # tanque de referência para traduzir R$/L em R$ por abastecimento

UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE",
       "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
CORES_REGIAO = {"N": "#2a9d8f", "NE": "#e9c46a", "CO": "#e76f51", "SE": "#264653", "S": "#8ab17d"}
NOMES_REGIAO = {"N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste", "SE": "Sudeste", "S": "Sul"}

NOME_EXIBICAO = {"BRASILIA": "Brasília", "GOIANIA": "Goiânia", "CUIABA": "Cuiabá", "BELEM": "Belém",
                 "MACAPA": "Macapá", "MACEIO": "Maceió", "VITORIA": "Vitória", "SAO LUIS": "São Luís",
                 "SAO PAULO": "São Paulo", "JOAO PESSOA": "João Pessoa", "FLORIANOPOLIS": "Florianópolis",
                 "RIO DE JANEIRO": "Rio de Janeiro", "PORTO ALEGRE": "Porto Alegre", "PORTO VELHO": "Porto Velho",
                 "RIO BRANCO": "Rio Branco", "BOA VISTA": "Boa Vista", "CAMPO GRANDE": "Campo Grande",
                 "BELO HORIZONTE": "Belo Horizonte", "ARACAJU": "Aracaju", "TERESINA": "Teresina"}
MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

def nome_cidade(m):
    return NOME_EXIBICAO.get(m, m.title())

plt.rcParams.update({"figure.dpi": 110, "savefig.dpi": 160, "font.size": 10,
                     "axes.spines.top": False, "axes.spines.right": False})

def salvar(fig, nome):
    tmp = f"/tmp/{nome}"
    fig.savefig(tmp, bbox_inches="tight", facecolor="white")
    shutil.copyfile(tmp, f"{PASTA_FIG}/{nome}")
    print("Figura salva em", f"{PASTA_FIG}/{nome}")

def mostrar(pdf, n=None):
    """display() de pandas com tudo convertido para texto (evita erro de conversão para Arrow)."""
    d = pdf if n is None else pdf.head(n)
    display(d.astype(str))

def br(x, casas=2):
    """Número no padrão brasileiro."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "-"
    return f"{x:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")

# visão analítica: fato com as dimensões
base = (spark.table(tab("fato_preco"))
        .join(spark.table(tab("dim_tempo")).select("sk_data", "data", "mes", "semana_anp_inicio"), "sk_data")
        .join(spark.table(tab("dim_local")), "sk_local")
        .join(spark.table(tab("dim_produto")), "sk_produto")
        .join(spark.table(tab("dim_bandeira")), "sk_bandeira")
        .withColumn("preco", F.col("valor_venda").cast("double")))
print(f"Base analítica: {base.count():,} preços".replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A · Qualidade por atributo
# MAGIC Para cada atributo de cada tabela do modelo: nulos, distintos, mínimo, máximo e quantos valores estão fora do domínio declarado no catálogo de dados.

# COMMAND ----------

# Domínio declarado no catálogo, escrito como condição que identifica valor FORA do domínio
c = F.col
DOMINIO = {
    ("fato_preco", "sk_data"): ~c("sk_data").between(20250101, 20251231),
    ("fato_preco", "sk_local"): c("sk_local") < 1,
    ("fato_preco", "sk_posto"): c("sk_posto") < 1,
    ("fato_preco", "sk_produto"): c("sk_produto") < 1,
    ("fato_preco", "sk_bandeira"): c("sk_bandeira") < 1,
    ("fato_preco", "valor_venda"): ~c("valor_venda").between(2, 15),
    ("fato_preco", "valor_compra"): ~c("valor_compra").between(0.5, 15),
    ("fato_preco", "arquivo_origem"): ~c("arquivo_origem").isin("ca-2025-01.csv", "ca-2025-02.csv"),
    ("dim_tempo", "sk_data"): ~c("sk_data").between(20250101, 20251231),
    ("dim_tempo", "data"): ~c("data").between("2025-01-01", "2025-12-31"),
    ("dim_tempo", "ano"): c("ano") != 2025,
    ("dim_tempo", "semestre"): ~c("semestre").isin(1, 2),
    ("dim_tempo", "trimestre"): ~c("trimestre").between(1, 4),
    ("dim_tempo", "mes"): ~c("mes").between(1, 12),
    ("dim_tempo", "nome_mes"): ~c("nome_mes").isin("JANEIRO", "FEVEREIRO", "MARCO", "ABRIL", "MAIO", "JUNHO", "JULHO",
                                                   "AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO"),
    ("dim_tempo", "dia"): ~c("dia").between(1, 31),
    ("dim_tempo", "dia_semana"): ~c("dia_semana").between(1, 7),
    ("dim_tempo", "nome_dia_semana"): ~c("nome_dia_semana").isin("DOMINGO", "SEGUNDA", "TERCA", "QUARTA", "QUINTA", "SEXTA", "SABADO"),
    ("dim_tempo", "semana_iso"): ~c("semana_iso").between(1, 53),
    ("dim_tempo", "ano_iso"): ~c("ano_iso").isin(2025, 2026),
    ("dim_tempo", "semana_anp_inicio"): (F.dayofweek("semana_anp_inicio") != 1) | ~c("semana_anp_inicio").between("2024-12-29", "2025-12-28"),
    ("dim_tempo", "eh_fim_de_semana"): c("eh_fim_de_semana") != F.dayofweek("data").isin(1, 7),
    ("dim_local", "regiao_sigla"): ~c("regiao_sigla").isin(*CORES_REGIAO.keys()),
    ("dim_local", "regiao_nome"): ~c("regiao_nome").isin("NORTE", "NORDESTE", "CENTRO-OESTE", "SUDESTE", "SUL"),
    ("dim_local", "uf"): ~c("uf").isin(*UFS),
    ("dim_local", "municipio"): c("municipio").rlike(r"[^A-Z0-9 '\-]"),
    ("dim_local", "sk_local"): c("sk_local") < 1,
    ("dim_posto", "sk_posto"): c("sk_posto") < 1,
    ("dim_posto", "cnpj"): ~c("cnpj").rlike(r"^[0-9]{14}$"),
    ("dim_posto", "cep"): ~c("cep").rlike(r"^[0-9]{8}$"),
    ("dim_posto", "primeira_coleta"): ~c("primeira_coleta").between("2025-01-01", "2025-12-31"),
    ("dim_posto", "ultima_coleta"): ~c("ultima_coleta").between("2025-01-01", "2025-12-31") | (c("ultima_coleta") < c("primeira_coleta")),
    ("dim_posto", "qtd_bandeiras"): c("qtd_bandeiras") < 1,
    ("dim_produto", "sk_produto"): c("sk_produto") < 1,
    ("dim_produto", "produto"): ~c("produto").rlike(r"^(GASOLINA|ETANOL|DIESEL|GNV)"),
    ("dim_produto", "grupo_produto"): ~c("grupo_produto").isin("GASOLINA", "ETANOL", "DIESEL", "GNV"),
    ("dim_produto", "unidade_medida"): ~c("unidade_medida").isin("R$ / litro", "R$ / m³"),
    ("dim_bandeira", "sk_bandeira"): c("sk_bandeira") < 1,
    ("dim_bandeira", "tipo_bandeira"): ~c("tipo_bandeira").isin("BRANCA", "COM BANDEIRA", "NAO INFORMADA"),
}

catalogo = spark.table(tab("catalogo_dados")).toPandas()
obrig = {(r.tabela, r.atributo): r.obrigatorio.startswith("Sim") for r in catalogo.itertuples()}

TABELAS = ["fato_preco", "dim_tempo", "dim_local", "dim_posto", "dim_produto", "dim_bandeira"]
linhas_qa = []
for t in TABELAS:
    df = spark.table(tab(t))
    n = df.count()
    aggs = []
    for fld in df.schema.fields:
        a = fld.name
        aggs += [F.sum(c(a).isNull().cast("int")).alias(f"{a}__nulos"),
                 F.countDistinct(a).alias(f"{a}__dist"),
                 F.min(a).cast("string").alias(f"{a}__min"),
                 F.max(a).cast("string").alias(f"{a}__max")]
        cond = DOMINIO.get((t, a))
        aggs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"{a}__fora") if cond is not None
                    else F.lit(None).cast("long").alias(f"{a}__fora"))
    r = df.agg(*aggs).first()
    for fld in df.schema.fields:
        a = fld.name
        nulos, fora = r[f"{a}__nulos"] or 0, r[f"{a}__fora"]
        ob = obrig.get((t, a), False)
        if ob and nulos > 0:
            situacao = "PROBLEMA: nulo em atributo obrigatório"
        elif fora is not None and fora > 0:
            situacao = "PROBLEMA: valor fora do domínio"
        elif nulos > 0:
            situacao = "OK (nulos permitidos)"
        else:
            situacao = "OK"
        linhas_qa.append({
            "tabela": t, "atributo": a, "tipo": fld.dataType.simpleString(), "obrigatorio": "Sim" if ob else "Não",
            "linhas": n, "nulos": nulos, "pct_nulos": round(100 * nulos / n, 2) if n else 0.0,
            "distintos": r[f"{a}__dist"], "minimo": (r[f"{a}__min"] or "")[:40], "maximo": (r[f"{a}__max"] or "")[:40],
            "fora_dominio": "sem regra (texto livre)" if fora is None else fora, "situacao": situacao})

qa = pd.DataFrame(linhas_qa)
spark.createDataFrame(qa.astype(str)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("qualidade_atributos"))
print(f"Atributos avaliados: {len(qa)} | com problema: {(qa['situacao'].str.startswith('PROBLEMA')).sum()}")
mostrar(qa)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B · Qualidade por dimensão

# COMMAND ----------

def cnpj_valido(cnpj):
    """Confere os dois dígitos verificadores do CNPJ."""
    if cnpj is None or len(cnpj) != 14 or not cnpj.isdigit() or len(set(cnpj)) == 1:
        return False
    d = [int(x) for x in cnpj]
    p1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    p2 = [6] + p1
    r1 = sum(a * b for a, b in zip(d[:12], p1)) % 11
    dv1 = 0 if r1 < 2 else 11 - r1
    r2 = sum(a * b for a, b in zip(d[:13], p2)) % 11
    dv2 = 0 if r2 < 2 else 11 - r2
    return d[12] == dv1 and d[13] == dv2

fato = spark.table(tab("fato_preco"))
n_fato = fato.count()
dim_posto = spark.table(tab("dim_posto"))
manifesto = spark.table(tab("manifesto_coleta")).toPandas()
excl = spark.table(tab("exclusoes_etl"))
n_lidas = int(manifesto.loc[manifesto["formato"] == "CSV", "linhas_dados"].sum())
dim = []  # (dimensão, indicador, resultado, avaliação)

# Completude
n_r0 = excl.where("regra = 'R0'").count()
nulos_obrig = int(qa.loc[qa["obrigatorio"] == "Sim", "nulos"].sum())
dim.append(("Completude", "Linhas totalmente vazias na fonte (R0)",
            f"{n_r0:,} de {n_lidas:,} ({100 * n_r0 / n_lidas:.2f}%)".replace(",", "."),
            "Excluídas no ETL; não carregam informação"))
dim.append(("Completude", "Nulos em atributos obrigatórios do modelo", str(nulos_obrig),
            "OK" if nulos_obrig == 0 else "Problema"))
for t, a in [("fato_preco", "valor_compra"), ("dim_posto", "complemento"), ("dim_posto", "cep"), ("dim_posto", "razao_social")]:
    lin = qa[(qa.tabela == t) & (qa.atributo == a)].iloc[0]
    dim.append(("Completude", f"Nulos em {t}.{a} (opcional)", f"{br(lin.pct_nulos)}%",
                "Atributo pouco preenchido; não é usado nas perguntas" if lin.pct_nulos > 50 else "Aceitável"))

# Unicidade
dup_fonte = excl.where("regra IN ('R14', 'R15')").count()
grao = fato.select("sk_posto", "sk_produto", "sk_data").distinct().count()
dim.append(("Unicidade", "Duplicatas na fonte (exatas ou conflitantes, R14 e R15)", str(dup_fonte),
            "OK" if dup_fonte == 0 else "Tratadas no ETL"))
dim.append(("Unicidade", "Grão da fato (posto, produto, data) único", f"{grao:,} de {n_fato:,}".replace(",", "."),
            "OK" if grao == n_fato else "Problema"))

# Consistência
multi_mun = fato.groupBy("sk_posto").agg(F.countDistinct("sk_local").alias("n")).where("n > 1").count()
compra_maior = fato.where(c("valor_compra").isNotNull() & (c("valor_compra") > c("valor_venda"))).count()
gas = (base.where(c("produto").isin("GASOLINA", "GASOLINA ADITIVADA"))
       .groupBy("sk_posto", "sk_data").pivot("produto", ["GASOLINA", "GASOLINA ADITIVADA"]).agg(F.max("preco")))
gas = gas.where(c("GASOLINA").isNotNull() & c("`GASOLINA ADITIVADA`").isNotNull())
n_par_gas = gas.count()
n_adit_menor = gas.where(c("`GASOLINA ADITIVADA`") < c("GASOLINA")).count()
dim.append(("Consistência", "Postos (CNPJ) em mais de um município", str(multi_mun), "OK" if multi_mun == 0 else "Verificar"))
dim.append(("Consistência", "Preço de compra maior que o de venda", str(compra_maior), "OK" if compra_maior == 0 else "Verificar"))
dim.append(("Consistência", "Gasolina aditivada mais barata que a comum (mesmo posto e data)",
            f"{n_adit_menor:,} de {n_par_gas:,} pares ({100 * n_adit_menor / max(n_par_gas, 1):.2f}%)".replace(",", "."),
            "OK" if n_adit_menor / max(n_par_gas, 1) < 0.01 else "Verificar"))

# Conformidade
fora_total = int(pd.to_numeric(qa["fora_dominio"], errors="coerce").fillna(0).sum())
cnpjs = [r["cnpj"] for r in dim_posto.select("cnpj").collect()]
cnpj_inv = sum(1 for x in cnpjs if not cnpj_valido(x))
dim.append(("Conformidade", "Valores fora do domínio declarado no catálogo (todas as tabelas)", str(fora_total),
            "OK" if fora_total == 0 else "Ver Parte A"))
dim.append(("Conformidade", "CNPJ com dígito verificador inválido", f"{cnpj_inv} de {len(cnpjs)}",
            "OK" if cnpj_inv == 0 else "Mantidos: identificam o posto de forma consistente na série"))

# Acurácia: preços atípicos pelo escore robusto (mediana e MAD) dentro de produto, UF e semana
g = base.groupBy("sk_produto", "uf", "semana_anp_inicio").agg(F.expr("percentile(preco, 0.5)").alias("med"))
b2 = base.join(g, ["sk_produto", "uf", "semana_anp_inicio"]).withColumn("desv", F.abs(c("preco") - c("med")))
mad = b2.groupBy("sk_produto", "uf", "semana_anp_inicio").agg(F.expr("percentile(desv, 0.5)").alias("mad"))
b2 = b2.join(mad, ["sk_produto", "uf", "semana_anp_inicio"]).withColumn("z", F.expr("try_divide(0.6745 * desv, mad)"))
n_z5 = b2.where(F.abs(c("z")) > 5).count()
dim.append(("Acurácia", "Preços fora da faixa plausível [2; 15] (R10)", str(excl.where("regra = 'R10'").count()), "OK"))
dim.append(("Acurácia", "Preços atípicos: escore robusto |z| > 5 em produto, UF e semana",
            f"{n_z5:,} ({100 * n_z5 / n_fato:.2f}%)".replace(",", "."),
            "Mantidos: plausíveis e as análises usam mediana e percentis, pouco sensíveis a eles"))

# Atualidade
lim = spark.table(tab("dim_tempo")).join(fato.select("sk_data").distinct(), "sk_data").agg(
    F.min("data").alias("dmin"), F.max("data").alias("dmax"), F.countDistinct("semana_anp_inicio").alias("sem")).first()
data_extr = pd.to_datetime(manifesto.loc[manifesto["formato"] == "CSV", "data_coleta_utc"].str[:10]).max().date()
dim.append(("Atualidade", "Período coberto pelas coletas", f"{lim['dmin']} a {lim['dmax']} ({lim['sem']} semanas ANP)", "OK"))
dim.append(("Atualidade", "Defasagem entre a última coleta e a extração",
            f"{(data_extr - lim['dmax']).days} dias (extração em {data_extr})",
            "Adequada para análise de 2025; para uso contínuo, ver autoavaliação"))

qd = pd.DataFrame(dim, columns=["dimensao", "indicador", "resultado", "avaliacao"])
spark.createDataFrame(qd).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("qualidade_dimensoes"))
mostrar(qd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C · Respostas às perguntas
# MAGIC As funções abaixo trabalham sobre tabelas pequenas já agregadas no Spark (por UF e semana, ou por município).

# COMMAND ----------

# Funções de análise (pandas e matplotlib)
def resume_p1(sem):
    """sem: uma linha por UF e semana com med_etanol, med_gasolina, razao. Devolve o resumo por UF."""
    linhas = []
    for (reg, uf), d in sem.groupby(["regiao_sigla", "uf"]):
        x = d["razao"].dropna().to_numpy()
        p = np.nan
        if TEM_SCIPY and len(x) >= 6 and np.any(x != LIMIAR_ETANOL):
            p = stats.wilcoxon(x - LIMIAR_ETANOL).pvalue
        comp = int((x <= LIMIAR_ETANOL).sum())
        linhas.append({"regiao_sigla": reg, "uf": uf, "semanas_validas": len(x), "semanas_compensou": comp,
                       "pct_semanas_compensou": round(100 * comp / len(x), 1) if len(x) else np.nan,
                       "razao_mediana": round(float(np.median(x)), 3) if len(x) else np.nan,
                       "razao_p25": round(float(np.percentile(x, 25)), 3) if len(x) else np.nan,
                       "razao_p75": round(float(np.percentile(x, 75)), 3) if len(x) else np.nan,
                       "p_wilcoxon_vs_070": p})
    r = pd.DataFrame(linhas).sort_values(["pct_semanas_compensou", "razao_mediana"], ascending=[False, True])
    r["compensou_na_maioria"] = r["pct_semanas_compensou"] > 50
    return r.reset_index(drop=True)

def grafico_p1_barras(res):
    d = res.sort_values(["pct_semanas_compensou", "razao_mediana"], ascending=[True, False])
    fig, ax = plt.subplots(figsize=(8, 7.5))
    ax.barh(d["uf"], d["pct_semanas_compensou"], color=[CORES_REGIAO[r] for r in d["regiao_sigla"]])
    ax.axvline(50, color="black", ls="--", lw=1, zorder=0)
    for i, (v, rz) in enumerate(zip(d["pct_semanas_compensou"], d["razao_mediana"])):
        ax.text(v + 1, i, f"{br(v, 0)}%  (razão mediana {br(rz, 2)})", va="center", fontsize=7.5,
                bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.5})
    ax.set_xlim(0, 125)
    ax.set_xlabel("% das semanas de 2025 em que etanol / gasolina ≤ 0,70 (tracejado: metade das semanas)")
    ax.set_title("P1. Em quantas semanas o etanol compensou, por UF (2025)", loc="left", fontweight="bold")
    handles = [plt.Rectangle((0, 0), 1, 1, color=v) for v in CORES_REGIAO.values()]
    ax.legend(handles, [NOMES_REGIAO[k] for k in CORES_REGIAO], loc="lower right", frameon=False, fontsize=8)
    return fig

def grafico_p1_mapa(sem, res):
    ordem = res.sort_values("razao_mediana")["uf"].tolist()
    m = sem.pivot_table(index="uf", columns="semana_anp_inicio", values="razao").reindex(ordem)
    fig, ax = plt.subplots(figsize=(11, 7))
    norm = TwoSlopeNorm(vmin=0.55, vcenter=LIMIAR_ETANOL, vmax=0.85)
    im = ax.imshow(m.to_numpy(dtype=float), aspect="auto", cmap="RdYlGn_r", norm=norm)
    ax.set_yticks(range(len(m.index)))
    ax.set_yticklabels(m.index, fontsize=8)
    cols = pd.to_datetime(pd.Series(m.columns))
    ano = cols.dt.year.mode()[0]  # a 1ª semana ANP começa em dezembro do ano anterior; não rotular
    pos = [i for i, d in enumerate(cols) if d.year == ano and (i == 0 or d.month != cols[i - 1].month)]
    ax.set_xticks(pos)
    ax.set_xticklabels([MESES[cols[i].month - 1] for i in pos], fontsize=8)
    ax.set_xlabel("semana da pesquisa ANP (domingo a sábado)")
    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.set_label("razão etanol / gasolina (verde: etanol compensa)")
    ax.set_title("P1. Razão semanal etanol / gasolina por UF (ordenado pela razão mediana)", loc="left", fontweight="bold")
    return fig

def resume_p2(mun):
    """mun: uma linha por município com med_bandeira, med_branca. Devolve (estatísticas, por região)."""
    d = mun.copy()
    d["dif"] = d["med_bandeira"] - d["med_branca"]
    d["dif_pct"] = 100 * d["dif"] / d["med_branca"]
    est = {"municipios": len(d), "dif_mediana": float(d["dif"].median()),
           "dif_p25": float(d["dif"].quantile(0.25)), "dif_p75": float(d["dif"].quantile(0.75)),
           "dif_pct_mediana": float(d["dif_pct"].median()),
           "pct_municipios_bandeira_mais_cara": float(100 * (d["dif"] > 0).mean()),
           "municipios_dif_zero": int((d["dif"] == 0).sum()), "p_wilcoxon": np.nan, "estatistica": np.nan}
    if TEM_SCIPY and len(d) >= 6 and np.any(d["dif"] != 0):
        t = stats.wilcoxon(d["dif"], alternative="greater")
        est["p_wilcoxon"], est["estatistica"] = float(t.pvalue), float(t.statistic)
    reg = (d.groupby("regiao_sigla").agg(municipios=("dif", "size"), dif_mediana=("dif", "median"),
                                         pct_bandeira_mais_cara=("dif", lambda s: 100 * (s > 0).mean()))
           .reindex(list(CORES_REGIAO)).dropna(how="all").reset_index())
    return d, est, reg

def grafico_p2(d, est):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), gridspec_kw={"width_ratios": [1.3, 1]})
    ax = axes[0]
    cent = 100 * d["dif"]
    ax.hist(cent, bins=40, color="#264653", alpha=0.85)
    ax.axvline(0, color="black", lw=1)
    ax.axvline(100 * est["dif_mediana"], color="#e76f51", lw=2, ls="--",
               label=f"mediana: {br(100 * est['dif_mediana'], 1)} centavos/L")
    ax.set_xlabel("diferença por município: bandeira − branca (centavos por litro)")
    ax.set_ylabel("municípios")
    ax.legend(frameon=False)
    ax.set_title("P2. Gasolina comum: postos com bandeira x bandeira branca", loc="left", fontweight="bold")
    ax = axes[1]
    regs = [r for r in CORES_REGIAO if r in set(d["regiao_sigla"])]
    dados = [100 * d.loc[d["regiao_sigla"] == r, "dif"].to_numpy() for r in regs]
    bp = ax.boxplot(dados, patch_artist=True, showfliers=False, widths=0.6, medianprops={"color": "black"})
    ax.set_xticks(range(1, len(regs) + 1))
    ax.set_xticklabels([NOMES_REGIAO[r] for r in regs], fontsize=8.5)
    for patch, r in zip(bp["boxes"], regs):
        patch.set_facecolor(CORES_REGIAO[r])
        patch.set_alpha(0.8)
    ax.axhline(0, color="black", lw=1)
    ax.set_ylabel("centavos por litro")
    ax.set_title("Por região (sem valores extremos)", loc="left", fontsize=10)
    fig.tight_layout()
    return fig

def resume_p3(sw):
    """sw: uma linha por município e semana com p10, p50, p90, n_postos. Devolve o resumo por município."""
    d = sw.copy()
    d["amp"] = d["p90"] - d["p10"]
    d["amp_pct"] = 100 * d["amp"] / d["p50"]
    r = (d.groupby(["regiao_sigla", "uf", "municipio", "eh_capital"])
         .agg(semanas=("amp", "size"), postos_mediana=("n_postos", "median"), preco_mediano=("p50", "median"),
              amp_mediana=("amp", "median"), amp_pct_mediana=("amp_pct", "median"))
         .reset_index().sort_values("amp_pct_mediana", ascending=False).reset_index(drop=True))
    r["economia_tanque"] = r["amp_mediana"] * TANQUE_L
    r["relevante"] = r["amp_pct_mediana"] >= 100 * LIMIAR_DISPERSAO
    return d, r

def grafico_p3_capitais(r):
    cap = r[r["eh_capital"]].sort_values("amp_pct_mediana")
    fig, ax = plt.subplots(figsize=(8, 7.5))
    cores = ["#e76f51" if m == "BRASILIA" else "#8d99ae" for m in cap["municipio"]]
    ax.barh([f"{nome_cidade(m)} ({u})" for m, u in zip(cap["municipio"], cap["uf"])], cap["amp_pct_mediana"], color=cores)
    ax.axvline(100 * LIMIAR_DISPERSAO, color="black", ls="--", lw=1, zorder=0)
    for i, (v, e) in enumerate(zip(cap["amp_pct_mediana"], cap["economia_tanque"])):
        ax.text(v + 0.1, i, f"{br(v, 1)}%  (R$ {br(e, 2)} por tanque de {TANQUE_L} L)", va="center", fontsize=7.5,
                bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.5})
    ax.set_xlim(0, max(cap["amp_pct_mediana"].max() * 1.6, 100 * LIMIAR_DISPERSAO * 1.6))
    ax.set_xlabel("amplitude P90 − P10 da gasolina comum, em % da mediana das semanas\n(tracejado: limiar de relevância de 5%)")
    ax.set_title("P3. Diferença entre postos baratos e caros nas capitais (2025)", loc="left", fontweight="bold")
    return fig

def grafico_p3_brasilia(d):
    b = d[d["municipio"] == "BRASILIA"].sort_values("semana_anp_inicio")
    x = pd.to_datetime(b["semana_anp_inicio"])
    fig, ax = plt.subplots(figsize=(10, 4.2))
    ax.fill_between(x, b["p10"], b["p90"], color="#e76f51", alpha=0.25, label="faixa entre P10 e P90 dos postos")
    ax.plot(x, b["p50"], color="#e76f51", lw=2, label="mediana")
    ax.plot(x, b["p10"], color="#e76f51", lw=0.8, ls=":")
    ax.plot(x, b["p90"], color="#e76f51", lw=0.8, ls=":")
    ax.set_ylabel("R$ por litro")
    meses = pd.date_range(x.min().normalize().replace(day=1), x.max(), freq="MS")
    meses = [m for m in meses if m >= x.min()]
    ax.set_xticks(meses)
    ax.set_xticklabels([MESES[m.month - 1] for m in meses])
    ax.legend(frameon=False, loc="upper left")
    ax.set_title("P3. Brasília: preço semanal da gasolina comum entre os postos pesquisados", loc="left", fontweight="bold")
    return fig

# COMMAND ----------

# MAGIC %md
# MAGIC ### P1 · Em quais UFs, e em quantas semanas, o etanol compensou?

# COMMAND ----------

p1_sem_sp = (base.where(c("produto").isin("GASOLINA", "ETANOL"))
    .groupBy("regiao_sigla", "uf", "semana_anp_inicio")
    .agg(F.expr("percentile(CASE WHEN produto = 'ETANOL' THEN preco END, 0.5)").alias("med_etanol"),
         F.expr("percentile(CASE WHEN produto = 'GASOLINA' THEN preco END, 0.5)").alias("med_gasolina"),
         F.sum(F.when(c("produto") == "ETANOL", 1).otherwise(0)).alias("n_etanol"),
         F.sum(F.when(c("produto") == "GASOLINA", 1).otherwise(0)).alias("n_gasolina"))
    .where((c("n_etanol") >= MIN_PRECOS_SEMANA) & (c("n_gasolina") >= MIN_PRECOS_SEMANA))
    .withColumn("razao", F.expr("try_divide(med_etanol, med_gasolina)")))
p1_sem = p1_sem_sp.toPandas()
p1_sem["semana_anp_inicio"] = pd.to_datetime(p1_sem["semana_anp_inicio"])
p1 = resume_p1(p1_sem)
spark.createDataFrame(p1.astype(str)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("resp_p1_uf"))

n_ufs_comp = int(p1["compensou_na_maioria"].sum())
H1 = n_ufs_comp < len(p1) / 2
print(f"UFs analisadas: {len(p1)} | semanas ANP com dados: {p1_sem['semana_anp_inicio'].nunique()}")
print(f"UFs em que o etanol compensou na maioria das semanas: {n_ufs_comp}")
print("H1 (menos da metade das UFs):", "CONFIRMADA" if H1 else "REFUTADA")
mostrar(p1)
fig = grafico_p1_barras(p1); salvar(fig, "p1_semanas_etanol_compensa.png"); plt.show()
fig = grafico_p1_mapa(p1_sem, p1); salvar(fig, "p1_razao_semanal_uf.png"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### P2 · Postos com bandeira cobram mais que os de bandeira branca?

# COMMAND ----------

p2_mun = (base.where((c("produto") == "GASOLINA") & c("tipo_bandeira").isin("BRANCA", "COM BANDEIRA"))
    .groupBy("regiao_sigla", "uf", "municipio", "eh_capital")
    .agg(F.expr("percentile(CASE WHEN tipo_bandeira = 'COM BANDEIRA' THEN preco END, 0.5)").alias("med_bandeira"),
         F.expr("percentile(CASE WHEN tipo_bandeira = 'BRANCA' THEN preco END, 0.5)").alias("med_branca"),
         F.sum(F.when(c("tipo_bandeira") == "COM BANDEIRA", 1).otherwise(0)).alias("n_bandeira"),
         F.sum(F.when(c("tipo_bandeira") == "BRANCA", 1).otherwise(0)).alias("n_branca"))
    .where((c("n_bandeira") >= MIN_PRECOS_TIPO) & (c("n_branca") >= MIN_PRECOS_TIPO))).toPandas()
n_mun_gas = base.where(c("produto") == "GASOLINA").select("sk_local").distinct().count()
p2_d, p2_est, p2_reg = resume_p2(p2_mun)
spark.createDataFrame(p2_d.astype(str)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("resp_p2_municipios"))

H2 = p2_est["dif_mediana"] > 0 and (p2_est["p_wilcoxon"] < ALFA)
print(f"Municípios com os dois tipos de posto (≥ {MIN_PRECOS_TIPO} preços cada): {p2_est['municipios']} de {n_mun_gas} com gasolina")
print(f"Diferença mediana (bandeira − branca): R$ {br(p2_est['dif_mediana'], 3)}/L ({br(p2_est['dif_pct_mediana'], 2)}%)")
print(f"Municípios em que a bandeira é mais cara: {br(p2_est['pct_municipios_bandeira_mais_cara'], 1)}%")
print(f"Wilcoxon pareado unilateral: p = {p2_est['p_wilcoxon']:.2e}")
print("H2:", "CONFIRMADA" if H2 else "REFUTADA")
mostrar(p2_reg)
fig = grafico_p2(p2_d, p2_est); salvar(fig, "p2_bandeira_vs_branca.png"); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### P3 · Diferença entre postos baratos e caros no mesmo município, e Brasília entre as capitais

# COMMAND ----------

p3_sw = (base.where(c("produto") == "GASOLINA")
    .groupBy("regiao_sigla", "uf", "municipio", "eh_capital", "semana_anp_inicio")
    .agg(F.countDistinct("sk_posto").alias("n_postos"),
         F.expr("percentile(preco, array(0.1, 0.5, 0.9))").alias("q"))
    .where(c("n_postos") >= MIN_POSTOS)
    .select("regiao_sigla", "uf", "municipio", "eh_capital", "semana_anp_inicio", "n_postos",
            c("q").getItem(0).alias("p10"), c("q").getItem(1).alias("p50"), c("q").getItem(2).alias("p90"))).toPandas()
p3_sw["semana_anp_inicio"] = pd.to_datetime(p3_sw["semana_anp_inicio"])
p3_d, p3 = resume_p3(p3_sw)
spark.createDataFrame(p3.astype(str)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tab("resp_p3_municipios"))

amp_tipica = float(p3["amp_pct_mediana"].median())
H3 = amp_tipica >= 100 * LIMIAR_DISPERSAO
cap = p3[p3["eh_capital"]].sort_values("amp_pct_mediana", ascending=False).reset_index(drop=True)
bsb = cap[cap["municipio"] == "BRASILIA"]
pos_bsb = int(bsb.index[0]) + 1 if len(bsb) else None
p_mw = np.nan
if TEM_SCIPY and len(bsb):
    x = p3_d.loc[p3_d["municipio"] == "BRASILIA", "amp_pct"]
    y = p3_d.loc[p3_d["eh_capital"] & (p3_d["municipio"] != "BRASILIA"), "amp_pct"]
    p_mw = float(stats.mannwhitneyu(x, y, alternative="two-sided").pvalue)

print(f"Municípios com semanas de ≥ {MIN_POSTOS} postos: {len(p3)} | capitais: {len(cap)} de 27")
print(f"Amplitude típica (mediana entre municípios): {br(amp_tipica, 2)}% da mediana")
print(f"Municípios com amplitude ≥ 5%: {int(p3['relevante'].sum())} de {len(p3)}")
print("H3:", "CONFIRMADA" if H3 else "REFUTADA")
if len(bsb):
    b = bsb.iloc[0]
    print(f"Brasília: amplitude {br(b['amp_pct_mediana'], 2)}% (R$ {br(b['amp_mediana'], 3)}/L; "
          f"R$ {br(b['economia_tanque'], 2)} por tanque) | posição entre as capitais: {pos_bsb}º de {len(cap)} (1º = maior diferença)")
    print(f"Mann-Whitney (semanas de Brasília x semanas das demais capitais): p = {p_mw:.2e}")
mostrar(cap)
fig = grafico_p3_capitais(p3); salvar(fig, "p3_amplitude_capitais.png"); plt.show()
if len(bsb):
    fig = grafico_p3_brasilia(p3_d); salvar(fig, "p3_brasilia_semanal.png"); plt.show()

# COMMAND ----------

# RESULTADOS 1 de 3: qualidade dos dados
print("RESULTADOS 1/3: QUALIDADE DOS DADOS")
print(f"Atributos avaliados: {len(qa)} | OK: {(~qa['situacao'].str.startswith('PROBLEMA')).sum()} | "
      f"com problema: {qa['situacao'].str.startswith('PROBLEMA').sum()}")
for r in qa[qa["situacao"] != "OK"].itertuples():
    print(f"- {r.tabela}.{r.atributo}: {r.situacao} | nulos {br(r.pct_nulos)}% | fora do domínio: {r.fora_dominio}")
print("\nPor dimensão:")
for r in qd.itertuples():
    print(f"- [{r.dimensao}] {r.indicador}: {r.resultado} | {r.avaliacao}")

# COMMAND ----------

# RESULTADOS 2 de 3: P1 e P2
print("RESULTADOS 2/3")
print(f"P1. UFs analisadas: {len(p1)} | UFs em que o etanol compensou na maioria das semanas: {n_ufs_comp} "
      f"| H1 {'CONFIRMADA' if H1 else 'REFUTADA'}")
for r in p1.itertuples():
    print(f"- {r.uf} ({r.regiao_sigla}): compensou em {r.semanas_compensou} de {r.semanas_validas} semanas "
          f"({br(r.pct_semanas_compensou, 1)}%) | razão mediana {br(r.razao_mediana, 3)} "
          f"[P25 {br(r.razao_p25, 3)}; P75 {br(r.razao_p75, 3)}] | p Wilcoxon vs 0,70 = {r.p_wilcoxon_vs_070:.1e}")
print(f"\nP2. Municípios comparados: {p2_est['municipios']} | diferença mediana R$ {br(p2_est['dif_mediana'], 3)}/L "
      f"[P25 {br(p2_est['dif_p25'], 3)}; P75 {br(p2_est['dif_p75'], 3)}] = {br(p2_est['dif_pct_mediana'], 2)}% "
      f"| bandeira mais cara em {br(p2_est['pct_municipios_bandeira_mais_cara'], 1)}% dos municípios "
      f"| empates: {p2_est['municipios_dif_zero']} | Wilcoxon unilateral p = {p2_est['p_wilcoxon']:.2e} "
      f"| H2 {'CONFIRMADA' if H2 else 'REFUTADA'}")
print(f"    Em um tanque de {TANQUE_L} L: R$ {br(p2_est['dif_mediana'] * TANQUE_L, 2)}")
for r in p2_reg.itertuples():
    print(f"- {NOMES_REGIAO[r.regiao_sigla]}: {int(r.municipios)} municípios | diferença mediana R$ {br(r.dif_mediana, 3)}/L "
          f"| bandeira mais cara em {br(r.pct_bandeira_mais_cara, 1)}%")

# COMMAND ----------

# RESULTADOS 3 de 3: P3 e figuras
print("RESULTADOS 3/3")
print(f"P3. Municípios analisados: {len(p3)} | amplitude típica {br(amp_tipica, 2)}% "
      f"[P25 {br(p3['amp_pct_mediana'].quantile(0.25), 2)}%; P75 {br(p3['amp_pct_mediana'].quantile(0.75), 2)}%] "
      f"| ≥ 5%: {int(p3['relevante'].sum())} de {len(p3)} | H3 {'CONFIRMADA' if H3 else 'REFUTADA'}")
print("Capitais (1º = maior diferença entre postos):")
for i, r in cap.iterrows():
    print(f"{i + 1:>2}. {r['municipio']} ({r['uf']}): {br(r['amp_pct_mediana'], 2)}% | R$ {br(r['amp_mediana'], 3)}/L "
          f"| R$ {br(r['economia_tanque'], 2)} por tanque | preço mediano R$ {br(r['preco_mediano'], 2)} "
          f"| {int(r['semanas'])} semanas, {br(r['postos_mediana'], 0)} postos por semana")
if len(bsb):
    print(f"Brasília x demais capitais (Mann-Whitney): p = {p_mw:.2e}")
print("\nFiguras salvas:")
for f in sorted(os.listdir(PASTA_FIG)):
    print(f"- {PASTA_FIG}/{f}")
