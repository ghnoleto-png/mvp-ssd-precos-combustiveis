# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Coleta (camada raw)
# MAGIC **MVP de Sistemas de Suporte à Decisão (UnB) · Preços de combustíveis da ANP · Gabriel Noleto (211068010)**
# MAGIC
# MAGIC O que este notebook faz:
# MAGIC 1. Cria o schema e o Volume no Unity Catalog.
# MAGIC 2. Baixa da ANP, **sem nenhuma alteração**, os arquivos semestrais de 2025 (.zip) e os metadados oficiais (.pdf).
# MAGIC 3. Extrai o CSV de cada .zip, também sem alteração.
# MAGIC 4. Gera um manifesto (data, volume, formato, hash SHA-256, número de linhas) em JSON e numa tabela Delta.
# MAGIC 5. Mostra as primeiras linhas de cada CSV, para conhecermos o formato real antes do ETL.
# MAGIC
# MAGIC Robustez: até 4 tentativas por arquivo, com espera crescente; arquivos já baixados e íntegros são pulados;
# MAGIC cada download é gravado primeiro como `.tmp` e só é renomeado para o nome final depois de conferido.
# MAGIC Pode ser executado de novo quantas vezes quiser.

# COMMAND ----------

# Configuração
CATALOG = "workspace"
SCHEMA = "mvp_combustiveis"
VOLUME = "raw"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PASTAS = {"zip": f"{BASE}/zip", "csv": f"{BASE}/csv", "docs": f"{BASE}/docs", "manifesto": f"{BASE}/manifesto"}

FONTE_PAGINA = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"
URL_BASE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc"
ARQUIVOS = [
    {"nome": "ca-2025-01.zip", "pasta": "zip", "url": f"{URL_BASE}/dsas/ca/ca-2025-01.zip",
     "descricao": "Combustíveis automotivos, 1º semestre de 2025"},
    {"nome": "ca-2025-02.zip", "pasta": "zip", "url": f"{URL_BASE}/dsas/ca/ca-2025-02.zip",
     "descricao": "Combustíveis automotivos, 2º semestre de 2025"},
    {"nome": "metadados-serie-historica-precos-combustiveis-1.pdf", "pasta": "docs",
     "url": f"{URL_BASE}/metadados-serie-historica-precos-combustiveis-1.pdf",
     "descricao": "Metadados oficiais da série (dicionário de dados da ANP)"},
]
TENTATIVAS = 4
TIMEOUT_S = 120
HEADERS = {"User-Agent": "Mozilla/5.0 (MVP-SSD-UnB; coleta academica)"}
LINHAS_AMOSTRA = 6

# COMMAND ----------

# Schema e Volume no Unity Catalog
import os

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
          f"COMMENT 'MVP SSD UnB: precos de combustiveis ANP'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} "
          f"COMMENT 'Camada raw: arquivos da ANP exatamente como baixados'")
for p in PASTAS.values():
    os.makedirs(p, exist_ok=True)
print("Schema e Volume prontos em", BASE)

# COMMAND ----------

# Funções auxiliares
import time, shutil, hashlib, json, zipfile, urllib.request, datetime as dt

def sha256_arquivo(caminho, bloco=1 << 20):
    h = hashlib.sha256()
    with open(caminho, "rb") as f:
        for b in iter(lambda: f.read(bloco), b""):
            h.update(b)
    return h.hexdigest()

def contar_linhas(caminho, bloco=1 << 20):
    """Conta linhas físicas do arquivo (inclui o cabeçalho)."""
    n, ultimo = 0, b""
    with open(caminho, "rb") as f:
        for b in iter(lambda: f.read(bloco), b""):
            n += b.count(b"\n")
            ultimo = b[-1:]
    if ultimo and ultimo != b"\n":
        n += 1  # última linha sem quebra no final
    return n

def arquivo_integro(caminho):
    if not os.path.exists(caminho) or os.path.getsize(caminho) == 0:
        return False
    if caminho.endswith(".zip"):
        try:
            with zipfile.ZipFile(caminho) as z:
                return z.testzip() is None
        except zipfile.BadZipFile:
            return False
    if caminho.endswith(".pdf"):
        with open(caminho, "rb") as f:
            return f.read(4) == b"%PDF"
    return True

def mover_para_final(tmp, final):
    try:
        os.replace(tmp, final)
    except OSError:  # fallback caso o sistema de arquivos não aceite renomear
        shutil.copyfile(tmp, final)
        os.remove(tmp)

def data_arquivo_utc(caminho):
    return dt.datetime.fromtimestamp(os.path.getmtime(caminho), tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def baixar(url, destino):
    """Baixa url para destino com novas tentativas e retomada. Retorna 'baixado' ou 'pulado'."""
    if arquivo_integro(destino):
        print(f"  já existe e está íntegro, pulando: {os.path.basename(destino)}")
        return "pulado"
    tmp = destino + ".tmp"
    for tentativa in range(1, TENTATIVAS + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r, open(tmp, "wb") as f:
                esperado = r.headers.get("Content-Length")
                shutil.copyfileobj(r, f, length=1 << 20)
            recebido = os.path.getsize(tmp)
            if esperado and int(esperado) != recebido:
                raise IOError(f"download incompleto: {recebido} de {esperado} bytes")
            mover_para_final(tmp, destino)
            if not arquivo_integro(destino):
                os.remove(destino)
                raise IOError("o arquivo baixado não passou na verificação de integridade")
            print(f"  baixado na tentativa {tentativa}: {os.path.basename(destino)} ({recebido / 1e6:.1f} MB)")
            return "baixado"
        except Exception as e:
            if os.path.exists(tmp):
                os.remove(tmp)
            print(f"  tentativa {tentativa}/{TENTATIVAS} falhou: {type(e).__name__}: {e}")
            if tentativa < TENTATIVAS:
                espera = 5 * 2 ** (tentativa - 1)
                print(f"  nova tentativa em {espera}s...")
                time.sleep(espera)
    raise RuntimeError(f"Falha definitiva ao baixar {url}. Plano B: upload manual para {os.path.dirname(destino)}")

def extrair_zip(caminho_zip, pasta_destino):
    """Extrai cada arquivo do zip sem alterar o conteúdo. Retorna [(caminho_final, nome_no_zip)]."""
    gerados = []
    stem = os.path.basename(caminho_zip)[:-4]
    with zipfile.ZipFile(caminho_zip) as z:
        membros = [i for i in z.infolist() if not i.is_dir()]
        for k, info in enumerate(membros, start=1):
            nome = os.path.basename(info.filename)
            ext = os.path.splitext(nome)[1].lower() or ".csv"
            if not (nome.lower().startswith(stem.lower()) and nome.isascii() and " " not in nome):
                # nome padronizado (sem acento/espaço) para evitar colisão e problemas no Spark;
                # o nome original fica registrado no manifesto
                nome = f"{stem}{ext}" if len(membros) == 1 else f"{stem}__{k}{ext}"
            final = os.path.join(pasta_destino, nome)
            if os.path.exists(final) and os.path.getsize(final) == info.file_size:
                print(f"  já extraído, pulando: {nome}")
            else:
                tmp = final + ".tmp"
                with z.open(info) as src, open(tmp, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1 << 20)
                mover_para_final(tmp, final)
                print(f"  extraído: {nome} ({info.file_size / 1e6:.1f} MB)")
            gerados.append((final, info.filename))
    return gerados

def detectar_formato(caminho, n_bytes=65536):
    """Detecta codificação e separador olhando o início do arquivo (só leitura)."""
    with open(caminho, "rb") as f:
        amostra = f.read(n_bytes)
    corte = amostra.rfind(b"\n")
    if corte > 0:
        amostra = amostra[:corte + 1]  # não cortar caractere no meio
    bom = amostra.startswith(b"\xef\xbb\xbf")
    try:
        amostra.decode("utf-8")
        enc = "utf-8-sig" if bom else "utf-8"
    except UnicodeDecodeError:
        enc = "latin-1"
    primeira = amostra.split(b"\n", 1)[0].decode(enc, errors="replace")
    contagem = {s: primeira.count(s) for s in [";", ",", "\t", "|"]}
    sep = max(contagem, key=contagem.get)
    return enc, sep, primeira.count(sep) + 1

# COMMAND ----------

# Download dos arquivos originais
registros = []  # linhas do manifesto
for a in ARQUIVOS:
    destino = os.path.join(PASTAS[a["pasta"]], a["nome"])
    print(f"\n{a['nome']}")
    status = baixar(a["url"], destino)
    registros.append({
        "arquivo": a["nome"], "camada": "raw", "caminho_volume": destino,
        "formato": a["nome"].rsplit(".", 1)[-1].upper(), "descricao": a["descricao"],
        "url_origem": a["url"], "arquivo_pai": None, "status_execucao": status,
    })

# COMMAND ----------

# Extração dos CSV (o .zip original continua guardado, intacto)
for z in [r for r in registros if r["formato"] == "ZIP"]:
    print(f"\n{z['arquivo']}")
    for caminho, nome_no_zip in extrair_zip(z["caminho_volume"], PASTAS["csv"]):
        registros.append({
            "arquivo": os.path.basename(caminho), "camada": "raw", "caminho_volume": caminho,
            "formato": os.path.basename(caminho).rsplit(".", 1)[-1].upper(),
            "descricao": f"Extraído de {z['arquivo']} (nome no zip: {nome_no_zip})",
            "url_origem": z["url_origem"], "arquivo_pai": z["arquivo"], "status_execucao": "extraido",
        })

# COMMAND ----------

# Manifesto: data, volume, formato, hash e linhas de cada arquivo
for r in registros:
    c = r["caminho_volume"]
    r["data_coleta_utc"] = data_arquivo_utc(c)
    r["tamanho_bytes"] = os.path.getsize(c)
    r["tamanho_mb"] = round(r["tamanho_bytes"] / 1e6, 2)
    r["sha256"] = sha256_arquivo(c)
    if r["formato"] in ("CSV", "TXT"):
        enc, sep, ncols = detectar_formato(c)
        total = contar_linhas(c)
        r.update({"linhas_total": total, "linhas_dados": total - 1, "colunas": ncols,
                  "encoding_detectado": enc, "separador": sep})
    else:
        r.update({"linhas_total": None, "linhas_dados": None, "colunas": None,
                  "encoding_detectado": None, "separador": None})
    r["fonte_pagina"] = FONTE_PAGINA

# JSON no Volume (grava .tmp e renomeia)
caminho_json = os.path.join(PASTAS["manifesto"], "manifesto_coleta.json")
with open(caminho_json + ".tmp", "w", encoding="utf-8") as f:
    json.dump(registros, f, ensure_ascii=False, indent=2)
mover_para_final(caminho_json + ".tmp", caminho_json)

# Tabela Delta no Unity Catalog
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
schema_manifesto = StructType([
    StructField("arquivo", StringType()), StructField("camada", StringType()),
    StructField("formato", StringType()), StructField("descricao", StringType()),
    StructField("data_coleta_utc", StringType()), StructField("tamanho_bytes", LongType()),
    StructField("tamanho_mb", DoubleType()), StructField("sha256", StringType()),
    StructField("linhas_total", LongType()), StructField("linhas_dados", LongType()),
    StructField("colunas", LongType()), StructField("encoding_detectado", StringType()),
    StructField("separador", StringType()), StructField("arquivo_pai", StringType()),
    StructField("caminho_volume", StringType()), StructField("url_origem", StringType()),
    StructField("fonte_pagina", StringType()), StructField("status_execucao", StringType()),
])
linhas = [tuple(r[c.name] for c in schema_manifesto.fields) for r in registros]
df_manifesto = spark.createDataFrame(linhas, schema_manifesto)
df_manifesto.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.manifesto_coleta")
print(f"Manifesto gravado em {caminho_json} e na tabela {CATALOG}.{SCHEMA}.manifesto_coleta")
display(df_manifesto.select("arquivo", "formato", "data_coleta_utc", "tamanho_mb", "linhas_dados",
                            "colunas", "encoding_detectado", "separador", "sha256"))

# COMMAND ----------

# Amostra das primeiras linhas de cada CSV (formato real, antes do ETL)
for r in [r for r in registros if r["formato"] == "CSV"]:
    print("=" * 100)
    print(f"{r['arquivo']} | encoding: {r['encoding_detectado']} | separador: '{r['separador']}' | colunas: {r['colunas']}")
    print("=" * 100)
    with open(r["caminho_volume"], "r", encoding=r["encoding_detectado"], errors="replace") as f:
        for i, linha in enumerate(f):
            if i >= LINHAS_AMOSTRA:
                break
            print(f"[{i}] {linha.rstrip()}")
    print()

# COMMAND ----------

# Evidência: conteúdo do Volume
for nome, p in PASTAS.items():
    print(f"\n{p}")
    for item in dbutils.fs.ls(p):
        print(f"  {item.name:60} {item.size / 1e6:10.2f} MB")

# COMMAND ----------

# Resumo em texto (para copiar)
print("RESUMO DA COLETA")
print(f"Fonte: {FONTE_PAGINA}")
for r in registros:
    linhas_txt = f"{r['linhas_dados']:,} linhas de dados".replace(",", ".") if r["linhas_dados"] is not None else "-"
    bytes_txt = f"{r['tamanho_bytes']:,}".replace(",", ".")
    print(f"- {r['arquivo']} | {r['formato']} | {bytes_txt} bytes | {linhas_txt} | "
          f"coleta {r['data_coleta_utc']} | sha256 {r['sha256']}")
total = sum(r["linhas_dados"] or 0 for r in registros if r["formato"] == "CSV")
print(f"Total de linhas de dados nos CSV: {total:,}".replace(",", "."))
