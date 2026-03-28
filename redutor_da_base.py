# Autor: Alberto Nagem — Cientista de Dados pela Universidade Presbiteriana Mackenzie
# Compacta em uma base csv.gz compativel com colab, bom como base de NB Jupyter

import pandas as pd
from pathlib import Path

INPUT = "dataset/pnadc_ocupados_2015_2025_base.parquet"
OUTPUT = "dataset/pnadc_ocupados_2015_2025_base.csv.gz"

# =========================
# COLUNAS ESSENCIAIS (EDA)
# =========================
cols = [
    "ano", "idade", "peso", "renda_habitual_real",
    "sexo_desc", "raca_cor_analitica", "escolaridade_desc", "grupo_h1",
    "ocup_h3", "atividade_h5", "posicao_ocupacao_desc",
    "formal_ampliado", "categoria_trabalho", "carteira_assinada",
    "cbo_detalhe_label", "uf_sigla"
]

print("📥 Lendo parquet...")
df = pd.read_parquet(INPUT, columns=[c for c in cols if c])

# =========================
# FILTROS (mesmos do EDA)
# =========================
df = df[
    (df["peso"] > 0) &
    (df["idade"].between(18, 65)) &
    (df["grupo_h1"].notna())
].copy()

# =========================
# OTIMIZAÇÃO DE TIPOS
# =========================
print("⚙️ Otimizando tipos...")

float_cols = ["renda_habitual_real", "peso"]
for c in float_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")

int_cols = ["ano", "idade"]
for c in int_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("int16")

cat_cols = [
    "sexo_desc", "raca_cor_analitica", "escolaridade_desc", "grupo_h1",
    "ocup_h3", "atividade_h5", "posicao_ocupacao_desc",
    "cbo_detalhe_label", "uf_sigla", "categoria_trabalho"
]

for c in cat_cols:
    if c in df.columns:
        df[c] = df[c].astype("category")

# =========================
# SALVAR CSV COMPRIMIDO
# =========================
print("💾 Salvando CSV otimizado...")
df.to_csv(OUTPUT, index=False, compression="gzip", encoding="utf-8-sig")

print("\n✅ FINALIZADO")
print(f"Tamanho reduzido drasticamente!")
