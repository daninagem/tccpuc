# Script de download e consolidação dos microdados do PNAD-C

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import bigquery
import google.auth


# =========================================================
# CONFIG
# =========================================================
PROJECT_ID = "dani-pucrs"
LOCATION = "US"

START_YEAR = 2015
END_YEAR = 2025

TABLE_ID = "basedosdados.br_ibge_pnadc.microdados"

SCRIPT_DIR = Path(__file__).resolve().parent
DICT_DIR = SCRIPT_DIR / "dicionario"
OUT_DIR = SCRIPT_DIR / "dataset"

OUT_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = OUT_DIR / "manifest_download.csv"
RESUMO_PATH = OUT_DIR / "resumo_download.csv"
CHOSEN_MAP_PATH = OUT_DIR / "schema_aliases.json"
COMBINED_PATH = OUT_DIR / f"pnadc_ocupados_{START_YEAR}_{END_YEAR}_base.parquet"

DEFLECTOR_XLS = DICT_DIR / "deflator_PNADC_2025_trimestral_070809.xls"
DIC_XLS = DICT_DIR / "dicionario_PNADC_microdados_trimestral.xls"
CBO_XLS = DICT_DIR / "Estrutura_Ocupacao_COD.xls"
CNAE_XLS = DICT_DIR / "Estrutura_Atividade_CNAE_Domiciliar_2_0.xls"

BUILD_COMBINED_PARQUET = True


# =========================================================
# LABELS BÁSICOS
# =========================================================
SEXO_LABELS = {
    "1": "Homem",
    "2": "Mulher",
    1: "Homem",
    2: "Mulher",
}

RACA_LABELS = {
    "1": "Branca",
    "2": "Preta",
    "3": "Amarela",
    "4": "Parda",
    "5": "Indígena",
    "9": "Ignorado/sem declaração",
    1: "Branca",
    2: "Preta",
    3: "Amarela",
    4: "Parda",
    5: "Indígena",
    9: "Ignorado/sem declaração",
}

ESCOLARIDADE_LABELS = {
    "1": "Sem instrução",
    "2": "Fundamental incompleto",
    "3": "Fundamental completo",
    "4": "Médio incompleto",
    "5": "Médio completo",
    "6": "Superior incompleto",
    "7": "Superior completo",
    1: "Sem instrução",
    2: "Fundamental incompleto",
    3: "Fundamental completo",
    4: "Médio incompleto",
    5: "Médio completo",
    6: "Superior incompleto",
    7: "Superior completo",
}

VD4008_LABELS = {
    "1": "Empregado no setor privado",
    "2": "Trabalhador doméstico",
    "3": "Empregado no setor público",
    "4": "Empregador",
    "5": "Conta própria",
    "6": "Trabalhador familiar auxiliar",
    1: "Empregado no setor privado",
    2: "Trabalhador doméstico",
    3: "Empregado no setor público",
    4: "Empregador",
    5: "Conta própria",
    6: "Trabalhador familiar auxiliar",
}

VD4009_LABELS = {
    "01": "Privado com carteira",
    "02": "Privado sem carteira",
    "03": "Doméstico com carteira",
    "04": "Doméstico sem carteira",
    "05": "Público com carteira",
    "06": "Público sem carteira",
    "07": "Militar/estatutário",
    "08": "Empregador",
    "09": "Conta própria",
    "10": "Auxiliar familiar",
    1: "Privado com carteira",
    2: "Privado sem carteira",
    3: "Doméstico com carteira",
    4: "Doméstico sem carteira",
    5: "Público com carteira",
    6: "Público sem carteira",
    7: "Militar/estatutário",
    8: "Empregador",
    9: "Conta própria",
    10: "Auxiliar familiar",
}

UF_LABELS = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE", "29": "BA",
    "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS",
    "50": "MS", "51": "MT", "52": "GO", "53": "DF",
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP",
    41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF",
}


# =========================================================
# AUTH / CLIENT
# =========================================================
def get_bq_client(project_id: str, location: str) -> bigquery.Client:
    try:
        credentials, _ = google.auth.default()
        return bigquery.Client(project=project_id, credentials=credentials, location=location)
    except Exception:
        print("\n❌ Não encontrei credenciais padrão (ADC).")
        print("   No PowerShell:")
        print("   1) gcloud auth application-default login")
        print(f"   2) gcloud config set project {project_id}\n")
        sys.exit(1)


# =========================================================
# SCHEMA DETECTION
# =========================================================
def get_table_fields(client: bigquery.Client, table_id: str) -> set[str]:
    tbl = client.get_table(table_id)
    return {f.name for f in tbl.schema}


def pick_first_existing(fields: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in fields:
            return c
    return None


def build_select_list(fields: set[str]) -> tuple[str, dict]:
    chosen = {
        "ano": "ano",
        "trimestre": "trimestre",
        "id_uf": pick_first_existing(fields, ["id_uf", "UF", "uf"]),
        "capital": pick_first_existing(fields, ["capital", "Capital"]),
        "rm_ride": pick_first_existing(fields, ["rm_ride", "RM_RIDE"]),
        "upa": pick_first_existing(fields, ["UPA", "id_upa", "upa"]),
        "v1008": pick_first_existing(fields, ["V1008", "v1008"]),
        "v1014": pick_first_existing(fields, ["V1014", "v1014"]),
        "v2003": pick_first_existing(fields, ["V2003", "v2003"]),

        "idade": pick_first_existing(fields, ["V2009", "v2009"]),
        "sexo": pick_first_existing(fields, ["V2007", "v2007"]),
        "raca_cor": pick_first_existing(fields, ["V2010", "v2010"]),
        "escolaridade": pick_first_existing(fields, ["VD3004", "vd3004"]),
        "anos_estudo": pick_first_existing(fields, ["VD3005", "vd3005"]),
        "grupo_anos_estudo": pick_first_existing(fields, ["VD3006", "vd3006"]),

        # educação detalhada para separar pós
        "v3009a_curso_mais_elevado": pick_first_existing(fields, ["V3009A", "v3009a"]),
        "v3009_curso_mais_elevado": pick_first_existing(fields, ["V3009", "v3009"]),
        "v3014_concluiu_curso": pick_first_existing(fields, ["V3014", "v3014"]),

        "condicao_ocupacao": pick_first_existing(fields, ["VD4002", "vd4002"]),
        "posicao_ocupacao_vd4007": pick_first_existing(fields, ["VD4007", "vd4007"]),
        "posicao_ocupacao_vd4008": pick_first_existing(fields, ["VD4008", "vd4008"]),
        "categoria_trabalho": pick_first_existing(fields, ["VD4009", "vd4009"]),
        "grupo_atividade_vd4010": pick_first_existing(fields, ["VD4010", "vd4010"]),
        "grupo_ocupacao_vd4011": pick_first_existing(fields, ["VD4011", "vd4011"]),
        "contrib_previdencia_vd4012": pick_first_existing(fields, ["VD4012", "vd4012"]),

        "v4010_cbo": pick_first_existing(fields, ["V4010", "v4010"]),
        "v4012_posicao_original": pick_first_existing(fields, ["V4012", "v4012"]),
        "v4013_cnae": pick_first_existing(fields, ["V4013", "v4013"]),
        "v4019_cnpj": pick_first_existing(fields, ["V4019", "v4019"]),
        "v4028_estatutario": pick_first_existing(fields, ["V4028", "v4028"]),
        "carteira_assinada": pick_first_existing(fields, ["V4029", "v4029"]),
        "v4032_previdencia_original": pick_first_existing(fields, ["V4032", "v4032"]),

        "renda_habitual": pick_first_existing(fields, ["VD4016", "vd4016"]),
        "renda_efetiva": pick_first_existing(fields, ["VD4017", "vd4017"]),
        "renda_habitual_todos": pick_first_existing(fields, ["VD4019", "vd4019"]),
        "renda_efetiva_todos": pick_first_existing(fields, ["VD4020", "vd4020"]),

        "horas_habituais_total": pick_first_existing(fields, ["VD4031", "vd4031"]),
        "horas_efetivas_total": pick_first_existing(fields, ["VD4035", "vd4035"]),
        "faixa_horas_hab": pick_first_existing(fields, ["VD4036", "vd4036"]),
        "faixa_horas_efe": pick_first_existing(fields, ["VD4037", "vd4037"]),

        "peso": pick_first_existing(fields, ["V1028", "v1028"]),
    }

    required_aliases = [
        "ano", "trimestre", "id_uf",
        "idade", "sexo", "raca_cor", "escolaridade",
        "condicao_ocupacao", "categoria_trabalho",
        "renda_habitual", "renda_efetiva", "peso",
        "v4010_cbo", "v4013_cnae"
    ]
    missing_required = [a for a in required_aliases if not chosen.get(a)]
    if missing_required:
        raise RuntimeError(
            "A tabela no BigQuery não contém campos essenciais para a EDA completa: "
            + ", ".join(missing_required)
        )

    select_parts = []
    for alias, field in chosen.items():
        if field:
            select_parts.append(f"{field} AS {alias}")

    return ",\n  ".join(select_parts), chosen


def build_query(select_sql: str, chosen_map: dict) -> str:
    cond_col = chosen_map["condicao_ocupacao"]
    return f"""
SELECT
  {select_sql}
FROM `{TABLE_ID}`
WHERE ano = @ano
  AND SAFE_CAST({cond_col} AS INT64) = 1
"""


# =========================================================
# APOIO LOCAL
# =========================================================
def require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo obrigatório não encontrado ({label}): {path}")


def load_deflator(deflator_xls: Path) -> pd.DataFrame:
    require_file(deflator_xls, "deflator")
    df = pd.read_excel(deflator_xls, sheet_name=0)

    df.columns = [str(c).strip() for c in df.columns]
    needed = {"Ano", "trim", "UF", "Habitual", "Efetivo"}
    if not needed.issubset(df.columns):
        raise RuntimeError(f"Deflator não tem as colunas esperadas: {needed}")

    trim_map = {
        "01-02-03": 1,
        "04-05-06": 2,
        "07-08-09": 3,
        "10-11-12": 4,
    }
    df["trimestre"] = df["trim"].astype(str).str.strip().map(trim_map)
    df = df[df["trimestre"].notna()].copy()

    df["ano"] = pd.to_numeric(df["Ano"], errors="coerce").astype("Int64")
    df["id_uf"] = pd.to_numeric(df["UF"], errors="coerce").astype("Int64")
    df["deflator_habitual"] = pd.to_numeric(df["Habitual"], errors="coerce")
    df["deflator_efetivo"] = pd.to_numeric(df["Efetivo"], errors="coerce")

    return df[["ano", "trimestre", "id_uf", "deflator_habitual", "deflator_efetivo"]].dropna()


def load_cbo_map(cbo_xls: Path) -> dict[int, str]:
    require_file(cbo_xls, "estrutura ocupação CBO")
    df = pd.read_excel(cbo_xls, sheet_name=0, header=None)

    code_col = 3
    label_col = 4
    out = {}

    for _, row in df.iterrows():
        code = row.iloc[code_col] if len(row) > code_col else np.nan
        label = row.iloc[label_col] if len(row) > label_col else np.nan

        if pd.isna(code) or pd.isna(label):
            continue

        code_str = str(code).strip()
        if code_str.isdigit() and len(code_str) == 4:
            out[int(code_str)] = str(label).strip()

    return out


def load_cnae_map(cnae_xls: Path) -> dict[int, str]:
    require_file(cnae_xls, "estrutura atividade CNAE")
    df = pd.read_excel(cnae_xls, sheet_name=0, header=None)

    code_col = 2
    label_col = 3
    out = {}

    for _, row in df.iterrows():
        code = row.iloc[code_col] if len(row) > code_col else np.nan
        label = row.iloc[label_col] if len(row) > label_col else np.nan

        if pd.isna(code) or pd.isna(label):
            continue

        code_str = str(code).strip()
        if code_str.isdigit() and len(code_str) == 5:
            out[int(code_str)] = str(label).strip()

    return out


# =========================================================
# CLASSIFICAÇÕES ANALÍTICAS
# =========================================================
def normalize_code_str(x, width: int | None = None) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s == "":
        return None
    if width is not None and s.isdigit():
        s = s.zfill(width)
    return s

def classify_escolaridade_analitica(row) -> str | None:

    esc = normalize_code_str(row.get("escolaridade"))
    curso = normalize_code_str(row.get("v3009a_curso_mais_elevado"), width=2)

    if esc is None:
        return None

    if curso is not None:
        try:
            curso_int = int(curso)
        except Exception:
            curso_int = None

        if curso_int is not None and 13 <= curso_int <= 15:
            return "Pós-graduação (ou equivalente)"

    mapping = {
        "1": "Sem instrução/Fundamental incompleto",
        "2": "Sem instrução/Fundamental incompleto",
        "3": "Fundamental completo",
        "4": "Médio incompleto",
        "5": "Médio completo",
        "6": "Superior incompleto",
        "7": "Superior completo",
    }
    return mapping.get(esc)


def classify_ocup_h3(v4010) -> str | None:
    if pd.isna(v4010):
        return None
    try:
        x = int(v4010)
    except Exception:
        return None

    if x == 0:
        return "11. Ocupações mal definidas"
    if 110 <= x <= 512:
        return "10. Forças armadas, policiais e bombeiros militares"
    if 1111 <= x <= 1439:
        return "1. Diretores e gerentes"
    if 2111 <= x <= 2659:
        return "2. Profissionais das ciências e intelectuais"
    if 3111 <= x <= 3522:
        return "3. Técnicos e profissionais de nível médio"
    if 4110 <= x <= 4419:
        return "4. Trabalhadores de apoio administrativo"
    if 5111 <= x <= 5419:
        return "5. Serviços, vendedores dos comércios e mercados"
    if 6111 <= x <= 6225:
        return "6. Agropecuária, florestais, caça e pesca"
    if 7111 <= x <= 7549:
        return "7. Construção, artes mecânicas e outros ofícios"
    if 8111 <= x <= 8350:
        return "8. Operadores de instalações e máquinas"
    if 9111 <= x <= 9629:
        return "9. Ocupações elementares"
    return None


def classify_atividade_h5(v4013) -> str | None:
    if pd.isna(v4013):
        return None
    try:
        x = int(v4013)
    except Exception:
        return None

    if x == 0:
        return "12. Atividades mal definidas"
    if 1101 <= x <= 3002:
        return "1. Agricultura, pecuária, produção florestal, pesca e aquicultura"
    if 5000 <= x <= 39000:
        return "2. Indústria geral"
    if 41000 <= x <= 43000:
        return "3. Construção"
    if 45010 <= x <= 48100:
        return "4. Comércio e reparação de veículos"
    if 49010 <= x <= 53002:
        return "5. Transporte, armazenagem e correio"
    if 55000 <= x <= 56020:
        return "6. Alojamento e alimentação"
    if 58000 <= x <= 82009:
        return "7. Informação, finanças, imobiliárias, profissionais e administrativas"
    if 84011 <= x <= 88000:
        return "8. Administração pública, educação, saúde e serviços sociais"
    if x == 97000:
        return "9. Serviços domésticos"
    if 90000 <= x <= 96090 or x == 99000:
        return "10. Outros serviços"
    return None


def build_grupo_interseccional(sexo_label: str, raca_label: str) -> str | None:
    if sexo_label not in {"Homem", "Mulher"}:
        return None
    if raca_label in {"Preta", "Parda"}:
        raca_bin = "Pretos/Pardos"
    elif raca_label == "Branca":
        raca_bin = "Branca"
    else:
        return None

    if sexo_label == "Homem" and raca_bin == "Branca":
        return "Homens Brancos"
    if sexo_label == "Homem" and raca_bin == "Pretos/Pardos":
        return "Homens Pretos/Pardos"
    if sexo_label == "Mulher" and raca_bin == "Branca":
        return "Mulheres Brancas"
    if sexo_label == "Mulher" and raca_bin == "Pretos/Pardos":
        return "Mulheres Pretas/Pardas"
    return None


# =========================================================
# LIMPEZA E ENRIQUECIMENTO
# =========================================================
def clean_min(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        "ano", "trimestre", "id_uf", "idade", "anos_estudo", "renda_habitual",
        "renda_efetiva", "renda_habitual_todos", "renda_efetiva_todos",
        "horas_habituais_total", "horas_efetivas_total", "peso",
        "v4010_cbo", "v4013_cnae"
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[df["peso"].notna() & (df["peso"] > 0)].copy()
    df = df[df["idade"].notna() & df["idade"].between(14, 90, inclusive="both")].copy()

    if "horas_habituais_total" in df.columns:
        df.loc[df["horas_habituais_total"].notna() & (df["horas_habituais_total"] < 0), "horas_habituais_total"] = np.nan
        df.loc[df["horas_habituais_total"].notna() & (df["horas_habituais_total"] > 120), "horas_habituais_total"] = np.nan

    if "horas_efetivas_total" in df.columns:
        df.loc[df["horas_efetivas_total"].notna() & (df["horas_efetivas_total"] < 0), "horas_efetivas_total"] = np.nan
        df.loc[df["horas_efetivas_total"].notna() & (df["horas_efetivas_total"] > 120), "horas_efetivas_total"] = np.nan

    return df


def add_labels_and_flags(
    df: pd.DataFrame,
    cbo_map: dict[int, str],
    cnae_map: dict[int, str],
    deflator_df: pd.DataFrame,
) -> pd.DataFrame:
    d = df.copy()

    for c in [
        "sexo", "raca_cor", "escolaridade",
        "posicao_ocupacao_vd4008", "posicao_ocupacao_vd4007",
        "carteira_assinada", "v4019_cnpj", "v4028_estatutario",
        "contrib_previdencia_vd4012", "v4032_previdencia_original",
        "v3009a_curso_mais_elevado", "v3009_curso_mais_elevado", "v3014_concluiu_curso",
    ]:
        if c in d.columns:
            d[c] = d[c].apply(normalize_code_str)

    if "categoria_trabalho" in d.columns:
        d["categoria_trabalho"] = d["categoria_trabalho"].apply(
            lambda x: normalize_code_str(x, width=2)
        )

    d["sexo_label"] = d["sexo"].map(SEXO_LABELS)
    d["raca_cor_label"] = d["raca_cor"].map(RACA_LABELS)
    d["escolaridade_label"] = d["escolaridade"].map(ESCOLARIDADE_LABELS)
    d["uf_sigla"] = d["id_uf"].map(UF_LABELS)

    d["escolaridade_desc_analitica"] = d.apply(classify_escolaridade_analitica, axis=1)
    d["escolaridade_desc"] = d["escolaridade_desc_analitica"]

    d["vd4008_label"] = d["posicao_ocupacao_vd4008"].map(VD4008_LABELS)
    d["vd4009_label"] = d["categoria_trabalho"].map(VD4009_LABELS)
    d["posicao_ocupacao_desc"] = d["vd4009_label"]

    d["raca_cor_analitica"] = np.where(
        d["raca_cor_label"].isin(["Preta", "Parda"]),
        "Pretos/Pardos",
        np.where(d["raca_cor_label"].eq("Branca"), "Branca", None),
    )
    d["sexo_desc"] = d["sexo_label"]

    d["grupo_h1"] = [
        build_grupo_interseccional(s, r)
        for s, r in zip(d["sexo_label"], d["raca_cor_label"])
    ]

    d["v4010_cbo"] = pd.to_numeric(d["v4010_cbo"], errors="coerce").astype("Int64")
    d["cbo_detalhe_label"] = d["v4010_cbo"].map(cbo_map)

    d["v4013_cnae"] = pd.to_numeric(d["v4013_cnae"], errors="coerce").astype("Int64")
    d["cnae_detalhe_label"] = d["v4013_cnae"].map(cnae_map)

    d["ocup_h3"] = d["v4010_cbo"].apply(classify_ocup_h3)
    d["atividade_h5"] = d["v4013_cnae"].apply(classify_atividade_h5)

    cat = d["categoria_trabalho"]

    d["is_privado_carteira"] = cat == "01"
    d["is_privado_sem_carteira"] = cat == "02"
    d["is_domestico_carteira"] = cat == "03"
    d["is_domestico_sem_carteira"] = cat == "04"
    d["is_publico_carteira"] = cat == "05"
    d["is_publico_sem_carteira"] = cat == "06"
    d["is_estatutario_militar"] = cat == "07"
    d["is_empregador"] = cat == "08"
    d["is_conta_propria"] = cat == "09"
    d["is_auxiliar_familiar"] = cat == "10"

    d["formal_empregado"] = d["categoria_trabalho"].isin(["01", "03", "05", "07"])

    d["tem_cnpj"] = d.get("v4019_cnpj", pd.Series(index=d.index)).eq("1")
    d["contribui_previdencia"] = d.get("contrib_previdencia_vd4012", pd.Series(index=d.index)).eq("1")

    d["formal_ampliado"] = (
        d["formal_empregado"]
        | (d["is_empregador"] & (d["tem_cnpj"] | d["contribui_previdencia"]))
        | (d["is_conta_propria"] & (d["tem_cnpj"] | d["contribui_previdencia"]))
    )

    d["tem_renda_habitual_pos"] = d["renda_habitual"].notna() & (d["renda_habitual"] > 0)
    d["tem_renda_efetiva_pos"] = d["renda_efetiva"].notna() & (d["renda_efetiva"] > 0)

    # ---------------------------------------------------------
    # Merge com deflator
    # ---------------------------------------------------------
    d["ano"] = pd.to_numeric(d["ano"], errors="coerce").astype("Int64")
    d["trimestre"] = pd.to_numeric(d["trimestre"], errors="coerce").astype("Int64")
    d["id_uf"] = pd.to_numeric(d["id_uf"], errors="coerce").astype("Int64")

    defl = deflator_df.copy()
    defl["ano"] = pd.to_numeric(defl["ano"], errors="coerce").astype("Int64")
    defl["trimestre"] = pd.to_numeric(defl["trimestre"], errors="coerce").astype("Int64")
    defl["id_uf"] = pd.to_numeric(defl["id_uf"], errors="coerce").astype("Int64")

    keep_defl = ["ano", "trimestre", "id_uf", "deflator_habitual", "deflator_efetivo"]
    defl = defl[keep_defl].copy()

    d = d.merge(
        defl,
        how="left",
        on=["ano", "trimestre", "id_uf"],
        validate="m:1",
        suffixes=("", "_defl")
    )

    if "deflator_habitual" not in d.columns and "deflator_habitual_defl" in d.columns:
        d["deflator_habitual"] = d["deflator_habitual_defl"]

    if "deflator_efetivo" not in d.columns and "deflator_efetivo_defl" in d.columns:
        d["deflator_efetivo"] = d["deflator_efetivo_defl"]

    if "deflator_habitual" not in d.columns or "deflator_efetivo" not in d.columns:
        raise RuntimeError(
            "Falha ao incorporar colunas do deflator após o merge. "
            f"Colunas disponíveis: {list(d.columns)}"
        )

    d["renda_habitual_real"] = np.where(
        d["renda_habitual"].notna() & d["deflator_habitual"].notna(),
        d["renda_habitual"] * d["deflator_habitual"],
        np.nan
    )

    d["renda_efetiva_real"] = np.where(
        d["renda_efetiva"].notna() & d["deflator_efetivo"].notna(),
        d["renda_efetiva"] * d["deflator_efetivo"],
        np.nan
    )

    if {"upa", "v1008", "v1014"}.issubset(d.columns):
        d["chave_domicilio"] = (
            d["upa"].astype(str).str.strip() + "_"
            + d["v1008"].astype(str).str.strip() + "_"
            + d["v1014"].astype(str).str.strip()
        )
    else:
        d["chave_domicilio"] = None

    if {"upa", "v1008", "v1014", "v2003"}.issubset(d.columns):
        d["chave_pessoa"] = (
            d["upa"].astype(str).str.strip() + "_"
            + d["v1008"].astype(str).str.strip() + "_"
            + d["v1014"].astype(str).str.strip() + "_"
            + d["v2003"].astype(str).str.strip()
        )
    else:
        d["chave_pessoa"] = None

    return d


# =========================================================
# DOWNLOAD / SAVE
# =========================================================
def download_year(client: bigquery.Client, query: str, year: int) -> tuple[pd.DataFrame, dict]:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("ano", "INT64", year)]
    )

    t0 = time.time()
    df = client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
    t1 = time.time()

    meta = {
        "ano": year,
        "linhas_raw": int(len(df)),
        "tempo_s": round(t1 - t0, 2),
    }
    return df, meta

def enforce_stable_schema(df: pd.DataFrame) -> pd.DataFrame:

    d = df.copy()

    string_cols = [
        "capital", "rm_ride", "upa", "v1008", "v1014",
        "sexo", "raca_cor", "escolaridade", "grupo_anos_estudo",
        "v3009a_curso_mais_elevado", "v3009_curso_mais_elevado", "v3014_concluiu_curso",
        "condicao_ocupacao", "posicao_ocupacao_vd4007", "posicao_ocupacao_vd4008",
        "categoria_trabalho", "grupo_atividade_vd4010", "grupo_ocupacao_vd4011",
        "contrib_previdencia_vd4012", "v4012_posicao_original", "v4019_cnpj",
        "v4028_estatutario", "carteira_assinada", "v4032_previdencia_original",
        "faixa_horas_hab", "faixa_horas_efe",
        "sexo_label", "raca_cor_label", "escolaridade_label", "uf_sigla",
        "escolaridade_desc_analitica", "escolaridade_desc",
        "vd4008_label", "vd4009_label", "posicao_ocupacao_desc",
        "raca_cor_analitica", "sexo_desc", "grupo_h1",
        "cbo_detalhe_label", "cnae_detalhe_label",
        "ocup_h3", "atividade_h5",
        "chave_domicilio", "chave_pessoa",
    ]

    int_cols = [
        "ano", "trimestre", "id_uf", "v2003", "idade", "anos_estudo",
        "v4010_cbo", "v4013_cnae",
        "horas_habituais_total", "horas_efetivas_total",
    ]

    float_cols = [
        "renda_habitual", "renda_efetiva", "renda_habitual_todos", "renda_efetiva_todos",
        "peso", "deflator_habitual", "deflator_efetivo",
        "renda_habitual_real", "renda_efetiva_real",
    ]

    bool_cols = [
        "is_privado_carteira", "is_privado_sem_carteira",
        "is_domestico_carteira", "is_domestico_sem_carteira",
        "is_publico_carteira", "is_publico_sem_carteira",
        "is_estatutario_militar", "is_empregador",
        "is_conta_propria", "is_auxiliar_familiar",
        "formal_empregado", "tem_cnpj", "contribui_previdencia",
        "formal_ampliado", "tem_renda_habitual_pos", "tem_renda_efetiva_pos",
    ]

    for c in string_cols:
        if c in d.columns:
            d[c] = d[c].astype("string")

    for c in int_cols:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").astype("Int64")

    for c in float_cols:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce").astype("float64")

    for c in bool_cols:
        if c in d.columns:
            d[c] = d[c].astype("boolean")

    return d

def save_year_parquet(df: pd.DataFrame, year: int) -> str:
    out_path = OUT_DIR / f"pnadc_ocupados_{year}.parquet"
    df.to_parquet(out_path, index=False)
    return str(out_path)


def append_to_combined_parquet(df: pd.DataFrame, path: Path, writer: pq.ParquetWriter | None) -> pq.ParquetWriter:
    table = pa.Table.from_pandas(df, preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter(path, table.schema, compression="snappy")
    writer.write_table(table)
    return writer


def build_quick_resumo(df: pd.DataFrame, year: int) -> dict:
    return {
        "ano": year,
        "linhas_pos_limpeza": int(len(df)),
        "trimestres_distintos": int(df["trimestre"].nunique(dropna=True)),
        "ufs_distintas": int(df["id_uf"].nunique(dropna=True)),
        "pct_renda_habitual_positiva": float((df["renda_habitual"].notna() & (df["renda_habitual"] > 0)).mean()),
        "pct_cbo_preenchido": float(df["v4010_cbo"].notna().mean()) if "v4010_cbo" in df.columns else np.nan,
        "pct_cnae_preenchido": float(df["v4013_cnae"].notna().mean()) if "v4013_cnae" in df.columns else np.nan,
        "pct_deflator_ok": float(df["deflator_habitual"].notna().mean()) if "deflator_habitual" in df.columns else np.nan,
        "pct_grupo_h1": float(df["grupo_h1"].notna().mean()) if "grupo_h1" in df.columns else np.nan,
        "pct_pos_separada": float(df["escolaridade_desc"].eq("Pós-graduação (ou equivalente)").mean()) if "escolaridade_desc" in df.columns else np.nan,
    }


# =========================================================
# MAIN
# =========================================================
def main():
    print("Conectando ao BigQuery...")
    client = get_bq_client(PROJECT_ID, LOCATION)
    print("✅ Conectado.")

    print("\n🔎 Lendo schema da tabela...")
    fields = get_table_fields(client, TABLE_ID)
    select_sql, chosen_map = build_select_list(fields)
    query = build_query(select_sql, chosen_map)

    with open(CHOSEN_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(chosen_map, f, ensure_ascii=False, indent=2)

    print("✅ Aliases detectados:")
    for k in sorted(chosen_map):
        if chosen_map[k]:
            print(f"   - {k:28s} <- {chosen_map[k]}")

    if not chosen_map.get("v3009a_curso_mais_elevado"):
        print("\n⚠️  ALERTA: não encontrei V3009A no schema desta tabela.")
        print("    A categoria 'Pós-graduação (ou equivalente)' não poderá ser separada corretamente.")
    else:
        print("\n✅ V3009A incluída no download (educação detalhada para separar pós).")

    print("\n📚 Lendo arquivos locais de apoio...")
    deflator_df = load_deflator(DEFLECTOR_XLS)
    cbo_map = load_cbo_map(CBO_XLS)
    cnae_map = load_cnae_map(CNAE_XLS)

    print(f"✅ Deflator carregado: {len(deflator_df):,} linhas")
    print(f"✅ CBO detalhado carregado: {len(cbo_map):,} códigos")
    print(f"✅ CNAE detalhado carregado: {len(cnae_map):,} códigos")

    manifest_rows = []
    resumo_rows = []
    combined_writer = None

    try:
        for year in range(START_YEAR, END_YEAR + 1):
            print(f"\n📥 Baixando ano {year}...")

            df, meta = download_year(client, query, year)

            if len(df) == 0:
                print(f"⚠️ Ano {year}: sem linhas.")
                manifest_rows.append({
                    "ano": year,
                    "status": "vazio",
                    "linhas_raw": 0,
                    "linhas_pos_limpeza": 0,
                    "arquivo": "",
                    "tempo_s": meta["tempo_s"],
                })
                continue

            print(f"✅ Ano {year}: {len(df):,} linhas raw. Tempo: {meta['tempo_s']}s")

            print("🧹 Limpeza mínima...")
            df = clean_min(df)
            print(f"✅ Pós-limpeza: {len(df):,} linhas")

            print("🧠 Enriquecendo base...")
            df = add_labels_and_flags(df, cbo_map, cnae_map, deflator_df)

            print("🧱 Padronizando schema...")
            df = enforce_stable_schema(df)

            print("💾 Salvando parquet anual...")
            out_file = save_year_parquet(df, year)
            print(f"✅ Salvo: {out_file}")

            if BUILD_COMBINED_PARQUET:
                combined_writer = append_to_combined_parquet(df, COMBINED_PATH, combined_writer)

            manifest_rows.append({
                "ano": year,
                "status": "ok",
                "linhas_raw": meta["linhas_raw"],
                "linhas_pos_limpeza": int(len(df)),
                "arquivo": out_file,
                "tempo_s": meta["tempo_s"],
            })

            resumo_rows.append(build_quick_resumo(df, year))
            del df

    finally:
        if combined_writer is not None:
            combined_writer.close()

    pd.DataFrame(manifest_rows).to_csv(MANIFEST_PATH, index=False, encoding="utf-8-sig")
    print(f"\n📄 Manifest salvo em: {MANIFEST_PATH}")

    if resumo_rows:
        pd.DataFrame(resumo_rows).to_csv(RESUMO_PATH, index=False, encoding="utf-8-sig")
        print(f"📄 Resumo salvo em: {RESUMO_PATH}")

    if BUILD_COMBINED_PARQUET and COMBINED_PATH.exists():
        print(f"📦 Base consolidada salva em: {COMBINED_PATH}")

    print("\n🎯 Pronto.")
    print(f"Saída principal: {OUT_DIR}")
    print("Script de Download de Microdados PNAD-C IBGE 2015-2025 por Alberto Nagem")


if __name__ == "__main__":
    main()