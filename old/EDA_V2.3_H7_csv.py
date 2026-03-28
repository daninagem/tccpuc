# -*- coding: utf-8 -*-
# Autor: Alberto Nagem — Cientista de Dados pela Universidade Presbiteriana Mackenzie

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from decimal import Decimal, getcontext

# =========================================================
# CONFIG
# =========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
DATASET_DIR = SCRIPT_DIR / "dataset"
OUTPUT_DIR = SCRIPT_DIR / "outputs"

DEFAULT_INPUT = DATASET_DIR / "pnadc_ocupados_2015_2025_base.csv.gz"

GROUP_ORDER = [
    "Homens Brancos",
    "Homens Pretos/Pardos",
    "Mulheres Brancas",
    "Mulheres Pretas/Pardas",
]

SCHOOL_ORDER = [
    "Sem instrução/Fundamental incompleto",
    "Fundamental completo",
    "Médio incompleto",
    "Médio completo",
    "Superior incompleto",
    "Superior completo",
    "Pós-graduação (ou equivalente)",
]

REGIAO_ORDER = ["Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"]

H3_ORDER = [
    "1. Diretores e gerentes",
    "2. Profissionais das ciências e intelectuais",
    "3. Técnicos e profissionais de nível médio",
    "4. Trabalhadores de apoio administrativo",
    "5. Serviços, vendedores dos comércios e mercados",
    "6. Agropecuária, florestais, caça e pesca",
    "7. Construção, artes mecânicas e outros ofícios",
    "8. Operadores de instalações e máquinas",
    "9. Ocupações elementares",
    "10. Forças armadas, policiais e bombeiros militares",
    "11. Ocupações mal definidas",
]

POS_ORDER = [
    "Privado com carteira",
    "Privado sem carteira",
    "Doméstico com carteira",
    "Doméstico sem carteira",
    "Público com carteira",
    "Público sem carteira",
    "Militar/estatutário",
    "Empregador",
    "Conta própria",
    "Auxiliar familiar",
]

HEATMAP_CMAP = "PuBuGn"
GROUP_COLORS = {
    "Homens Brancos": "#1f77b4",
    "Homens Pretos/Pardos": "#ff7f0e",
    "Mulheres Brancas": "#2ca02c",
    "Mulheres Pretas/Pardas": "#d62728",
}

HIPOTESES = {
    "H1": "A educação aumenta a renda para todos os grupos da mesma forma, ou a desigualdade persiste mesmo em níveis mais altos de escolaridade?",
    "H2": "O retorno da escolaridade (ganho adicional de renda por nível educacional) é igual entre os grupos, ou se torna mais desigual nos níveis mais altos?",
    "H3": "Diferentes grupos estão distribuídos de forma semelhante entre as ocupações, ou existe segregação ocupacional?",
    "H4": "O aumento da escolaridade leva à formalização do trabalho de forma igual para todos os grupos, ou a desigualdade permanece?",
    "H5": "As diferenças de renda entre grupos são explicadas pelo tipo de ocupação, ou persistem mesmo dentro das mesmas ocupações?",
    "H6": "Os Jovens de hoje ainda enfrentam as mesmas desigualdades do passado?",
    "H7": "O acesso a cargos de liderança é distribuído de forma proporcional entre os grupos, ou existe concentração em grupos específicos?",
}

# =========================================================
# ARGUMENTOS
# =========================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=str(DEFAULT_INPUT))
    p.add_argument("--out", default=str(OUTPUT_DIR))
    p.add_argument("--value-col", default="renda_habitual_real")
    p.add_argument("--weight-col", default="peso")
    p.add_argument("--idade-min", type=int, default=18)
    p.add_argument("--idade-max", type=int, default=65)

    p.add_argument("--show", dest="show", action="store_true")
    p.add_argument("--no-show", dest="show", action="store_false")
    p.set_defaults(show=True)

    p.add_argument("--winsorize", dest="winsorize", action="store_true")
    p.add_argument("--no-winsorize", dest="winsorize", action="store_false")
    p.set_defaults(winsorize=True)

    p.add_argument("--winsorize-q", type=float, default=0.99)
    return p.parse_args()

# =========================================================
# UTIL
# =========================================================
def print_block(code: str, desc: str | None = None, pergunta: str | None = None) -> None:
    title = code if desc is None else f"{code} — {desc}"
    print("\n" + "=" * 100)
    print(title)
    if pergunta:
        print(pergunta)
    print("=" * 100)

def ensure_dirs(root: Path) -> dict[str, Path]:
    dirs = {k: root / k for k in ("tables", "figures", "reports")}
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs

def show_or_close(fig, show: bool) -> None:
    if show:
        plt.show()
    else:
        plt.close(fig)

def fmt_money(x: float) -> str:
    if pd.isna(x):
        return "NA"
    s = f"{x:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def reorder_columns_if_present(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    existing = [c for c in cols if c in df.columns]
    remaining = [c for c in df.columns if c not in existing]
    return df[existing + remaining]

def reorder_index_if_present(df: pd.DataFrame, idx_order: Iterable[str]) -> pd.DataFrame:
    existing = [i for i in idx_order if i in df.index]
    remaining = [i for i in df.index if i not in existing]
    return df.loc[existing + remaining]

def styled_line(ax, x, y, label: str) -> None:
    ax.plot(
        x,
        y,
        marker="o",
        linewidth=2.3,
        label=str(label),
        color=GROUP_COLORS.get(str(label)),
    )

def fit_linear_trend(x, y) -> tuple[float, float]:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    mask = ~np.isnan(x) & ~np.isnan(y)
    if mask.sum() < 2:
        return np.nan, np.nan
    a, b = np.polyfit(x[mask], y[mask], 1)
    return float(a), float(b)

def wmean(x: pd.Series, w: pd.Series) -> float:
    getcontext().prec = 28
    x = pd.to_numeric(x, errors="coerce")
    w = pd.to_numeric(w, errors="coerce")

    mask = x.notna() & w.notna() & (w > 0)
    if not mask.any():
        return np.nan

    x_vals = x[mask].astype("float64").values
    w_vals = w[mask].astype("float64").values

    num = Decimal(0)
    den = Decimal(0)

    for xi, wi in zip(x_vals, w_vals):
        num += Decimal(str(xi)) * Decimal(str(wi))
        den += Decimal(str(wi))

    return float(num / den)

def precise_sum(series: pd.Series) -> float:
    getcontext().prec = 28
    total = Decimal(0)
    for v in pd.to_numeric(series, errors="coerce").dropna():
        total += Decimal(str(float(v)))
    return float(total)

def agg_wmean(df: pd.DataFrame, group_cols: list[str], value_col: str, weight_col: str) -> pd.DataFrame:
    df = df.sort_values(by=group_cols).copy()

    rows = []
    grouped = df.groupby(group_cols, dropna=False, observed=False)
    for keys, g in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {c: k for c, k in zip(group_cols, keys)}
        row["valor"] = wmean(g[value_col], g[weight_col])
        rows.append(row)
    return pd.DataFrame(rows)

def agg_weighted_share_binary(
    df: pd.DataFrame,
    group_cols: list[str],
    bin_col: str,
    weight_col: str,
    out_col: str,
) -> pd.DataFrame:
    df = df.sort_values(by=group_cols).copy()

    rows = []
    grouped = df.groupby(group_cols, dropna=False, observed=False)
    for keys, g in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {c: k for c, k in zip(group_cols, keys)}
        val = wmean(g[bin_col], g[weight_col])
        row[out_col] = val * 100 if pd.notna(val) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)

def wshare_table(df: pd.DataFrame, group_cols: list[str], cat_col: str, wcol: str) -> pd.DataFrame:
    tmp = df[group_cols + [cat_col, wcol]].copy()
    tmp = tmp.dropna(subset=[cat_col, wcol])
    tmp[wcol] = pd.to_numeric(tmp[wcol], errors="coerce")
    tmp = tmp[tmp[wcol] > 0]
    tmp = tmp.sort_values(by=group_cols + [cat_col]).copy()

    by_cat = (
        tmp.groupby(group_cols + [cat_col], dropna=False, observed=False)[wcol]
        .sum()
        .rename("w")
        .reset_index()
    )
    by_total = (
        tmp.groupby(group_cols, dropna=False, observed=False)[wcol]
        .sum()
        .rename("w_total")
        .reset_index()
    )
    out = by_cat.merge(by_total, on=group_cols, how="left")
    out["pct"] = (out["w"] / out["w_total"]) * 100
    return out

def add_bar_labels(ax, fmt="{:.1f}", rotation=0, fontsize=8) -> None:
    for container in ax.containers:
        labels = []
        for bar in container:
            h = bar.get_height()
            labels.append("" if np.isnan(h) or h == 0 else fmt.format(h))
        ax.bar_label(container, labels=labels, padding=2, rotation=rotation, fontsize=fontsize)

def write_report(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines), encoding="utf-8")

# =========================================================
# PLOTS
# =========================================================
def plot_series_por_grupo_com_tendencia(
    tab_ano_grupo: pd.DataFrame,
    title: str,
    outpath: Path,
    show: bool = True,
    y_label: str = "Valor",
) -> None:
    if tab_ano_grupo.empty:
        print(f"[WARN] {title}: tabela vazia.")
        return

    dfp = tab_ano_grupo.copy().sort_index()
    anos = dfp.index.to_numpy(dtype=float)

    fig, ax = plt.subplots(figsize=(11.5, 6.2))
    for col in dfp.columns:
        y = dfp[col].to_numpy(dtype=float)
        color = GROUP_COLORS.get(str(col))
        ax.plot(anos, y, marker="o", linewidth=2.3, label=str(col), color=color)
        a, b = fit_linear_trend(anos, y)
        if not np.isnan(a):
            ax.plot(anos, a * anos + b, linestyle="--", linewidth=1.5, color=color, alpha=0.8)

    ax.set_title(title)
    ax.set_xlabel("Ano")
    ax.set_ylabel(y_label)
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best", frameon=True)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    show_or_close(fig, show)

def plot_gap_com_tendencia(
    tab_ano_grupo: pd.DataFrame,
    ref_col: str,
    comp_col: str,
    title: str,
    outpath: Path,
    show: bool = True,
) -> None:
    dfp = tab_ano_grupo.copy().sort_index()
    if ref_col not in dfp.columns or comp_col not in dfp.columns:
        print(f"[WARN] {title}: colunas ausentes.")
        return

    anos = dfp.index.to_numpy(dtype=float)
    gap = (dfp[comp_col] / dfp[ref_col] - 1) * 100

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(anos.astype(int), gap.to_numpy(dtype=float), color="#4C8BB8")
    add_bar_labels(ax, fmt="{:.1f}", rotation=90, fontsize=7)

    a, b = fit_linear_trend(anos, gap.to_numpy(dtype=float))
    if not np.isnan(a):
        ax.plot(anos, a * anos + b, linestyle="--", linewidth=2, color="#4C8BB8")

    ax.set_title(title)
    ax.set_xlabel("Ano")
    ax.set_ylabel("Gap relativo (%)")
    ax.grid(True, axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    show_or_close(fig, show)

def plot_stacked_bar(
    piv: pd.DataFrame,
    title: str,
    outpath: Path,
    show: bool = True,
    ylabel: str = "Participação (%)",
) -> None:
    if piv.empty:
        print(f"[WARN] {title}: tabela vazia.")
        return

    fig, ax = plt.subplots(figsize=(12.5, 6.2))
    bottom = np.zeros(len(piv.index))
    x = np.arange(len(piv.index))

    for c in piv.columns:
        vals = piv[c].fillna(0).to_numpy()
        ax.bar(x, vals, bottom=bottom, label=c)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels(piv.index.tolist(), rotation=15, ha="right")
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, 100)
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(ncols=2, fontsize=8, frameon=True)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    show_or_close(fig, show)

def plot_heatmap_like(
    piv: pd.DataFrame,
    title: str,
    outpath: Path,
    show: bool = True,
    cmap: str = "Blues",
    fmt: str = ".1f",
) -> None:
    if piv.empty:
        print(f"[WARN] {title}: tabela vazia.")
        return

    fig, ax = plt.subplots(
        figsize=(max(7.8, 1.25 * len(piv.columns)), max(4.8, 0.82 * len(piv.index)))
    )
    data = piv.to_numpy(dtype=float)
    im = ax.imshow(data, aspect="auto", cmap=cmap)

    ax.set_xticks(np.arange(len(piv.columns)))
    ax.set_xticklabels(piv.columns, rotation=25, ha="right")
    ax.set_yticks(np.arange(len(piv.index)))
    ax.set_yticklabels(piv.index)

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            if np.isfinite(data[i, j]):
                ax.text(j, i, f"{data[i, j]:{fmt}}", ha="center", va="center", fontsize=8)

    ax.set_title(title)
    fig.colorbar(im, ax=ax, shrink=0.85)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    show_or_close(fig, show)

def plot_bar_rank(
    s: pd.Series,
    title: str,
    outpath: Path,
    show: bool = True,
    ylabel: str = "Valor",
    fmt: str = "{:.1f}",
) -> None:
    if s.empty:
        print(f"[WARN] {title}: série vazia.")
        return

    fig, ax = plt.subplots(figsize=(10.5, 5.2))
    ax.bar(s.index.astype(str).tolist(), s.values)
    add_bar_labels(ax, fmt=fmt, rotation=0, fontsize=8)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outpath, dpi=180)
    show_or_close(fig, show)

# =========================================================
# BASE
# =========================================================
def add_missing_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    d = df

    if "sexo_desc" not in d.columns and "sexo_label" in d.columns:
        d["sexo_desc"] = d["sexo_label"]

    if "raca_cor_analitica" not in d.columns and "raca_cor_label" in d.columns:
        d["raca_cor_analitica"] = np.where(
            d["raca_cor_label"].isin(["Preta", "Parda"]),
            "Pretos/Pardos",
            np.where(d["raca_cor_label"].eq("Branca"), "Branca", None),
        )

    if "escolaridade_desc" not in d.columns:
        if "escolaridade_desc_analitica" in d.columns:
            d["escolaridade_desc"] = d["escolaridade_desc_analitica"]
        elif "escolaridade_label" in d.columns:
            simple_map = {
                "Sem instrução": "Sem instrução/Fundamental incompleto",
                "Fundamental incompleto": "Sem instrução/Fundamental incompleto",
                "Fundamental completo": "Fundamental completo",
                "Médio incompleto": "Médio incompleto",
                "Médio completo": "Médio completo",
                "Superior incompleto": "Superior incompleto",
                "Superior completo": "Superior completo",
            }
            d["escolaridade_desc"] = d["escolaridade_label"].map(simple_map)

    if "posicao_ocupacao_desc" not in d.columns and "vd4009_label" in d.columns:
        d["posicao_ocupacao_desc"] = d["vd4009_label"]

    if "formal_ampliado" in d.columns:
        d["formal_ampliado"] = pd.to_numeric(d["formal_ampliado"], errors="coerce")

    if "categoria_trabalho" in d.columns:
        cat = d["categoria_trabalho"].astype("string").str.strip().str.zfill(2)
        d["formal_carteira"] = pd.Series(np.nan, index=d.index, dtype="float64")
        d.loc[cat.isin(["01", "03", "05", "07"]), "formal_carteira"] = 1.0
        d.loc[cat.isin(["02", "04", "06"]), "formal_carteira"] = 0.0
        d["formal_carteira_escopo"] = np.where(
            cat.isin(["01", "02", "03", "04", "05", "06", "07"]),
            1.0,
            np.nan,
        )
    elif "carteira_assinada" in d.columns:
        carteira_num = pd.to_numeric(d["carteira_assinada"], errors="coerce")
        d["formal_carteira"] = pd.Series(
            np.where(carteira_num.fillna(-1).eq(1), 1.0, 0.0),
            index=d.index,
            dtype="float64",
        )
        d.loc[carteira_num.isna(), "formal_carteira"] = np.nan
        d["formal_carteira_escopo"] = np.where(carteira_num.notna(), 1.0, np.nan)

    if "grupo_h1" not in d.columns and {"sexo_desc", "raca_cor_analitica"}.issubset(d.columns):
        d["grupo_h1"] = np.select(
            [
                (d["sexo_desc"] == "Mulher") & (d["raca_cor_analitica"] == "Pretos/Pardos"),
                (d["sexo_desc"] == "Homem") & (d["raca_cor_analitica"] == "Branca"),
                (d["sexo_desc"] == "Mulher") & (d["raca_cor_analitica"] == "Branca"),
                (d["sexo_desc"] == "Homem") & (d["raca_cor_analitica"] == "Pretos/Pardos"),
            ],
            [
                "Mulheres Pretas/Pardas",
                "Homens Brancos",
                "Mulheres Brancas",
                "Homens Pretos/Pardos",
            ],
            default=None,
        )

    if "regiao_desc" not in d.columns and "uf_sigla" in d.columns:
        reg_map = {
            "RO": "Norte", "AC": "Norte", "AM": "Norte", "RR": "Norte", "PA": "Norte", "AP": "Norte", "TO": "Norte",
            "MA": "Nordeste", "PI": "Nordeste", "CE": "Nordeste", "RN": "Nordeste", "PB": "Nordeste", "PE": "Nordeste",
            "AL": "Nordeste", "SE": "Nordeste", "BA": "Nordeste",
            "MG": "Sudeste", "ES": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
            "PR": "Sul", "SC": "Sul", "RS": "Sul",
            "MS": "Centro-Oeste", "MT": "Centro-Oeste", "GO": "Centro-Oeste", "DF": "Centro-Oeste",
        }
        d["regiao_desc"] = d["uf_sigla"].map(reg_map)

    return d

def prepare_base(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str,
    idade_min: int,
    idade_max: int,
) -> pd.DataFrame:
    d = add_missing_derived_columns(df)

    needed = [
        "ano", "idade", weight_col, value_col,
        "sexo_desc", "raca_cor_analitica", "escolaridade_desc", "grupo_h1",
    ]
    missing = [c for c in needed if c not in d.columns]
    if missing:
        raise ValueError(f"Colunas ausentes na base para EDA: {missing}")

    for c in ["ano", "idade", weight_col, value_col, "formal_ampliado", "formal_carteira", "formal_carteira_escopo"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    cat_cols = [
        "sexo_desc", "raca_cor_analitica", "escolaridade_desc", "grupo_h1",
        "ocup_h3", "atividade_h5", "posicao_ocupacao_desc",
        "cbo_detalhe_label", "uf_sigla", "regiao_desc",
    ]
    for c in cat_cols:
        if c in d.columns:
            d[c] = d[c].astype("category")

    mask = (
        d[weight_col].notna()
        & (d[weight_col] > 0)
        & d["idade"].notna()
        & d["idade"].between(idade_min, idade_max, inclusive="both")
        & d["grupo_h1"].notna()
    )
    d = d.loc[mask].copy()

    if "escolaridade_desc" in d.columns:
        d["escolaridade_desc"] = pd.Categorical(
            d["escolaridade_desc"].astype("string"),
            categories=SCHOOL_ORDER,
            ordered=True,
        )

    if "regiao_desc" in d.columns:
        d["regiao_desc"] = pd.Categorical(
            d["regiao_desc"].astype("string"),
            categories=REGIAO_ORDER,
            ordered=True,
        )

    return d

# =========================================================
# H1
# =========================================================
def bloco_h1(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    value_col: str,
    weight_col: str,
    show: bool,
    report: list[str],
) -> None:
    print_block("H1", "Renda por escolaridade", HIPOTESES["H1"])

    tab = agg_wmean(df_table, ["escolaridade_desc", "grupo_h1"], value_col, weight_col)
    piv = tab.pivot(index="escolaridade_desc", columns="grupo_h1", values="valor")
    piv = reorder_index_if_present(piv, SCHOOL_ORDER)
    piv = reorder_columns_if_present(piv, GROUP_ORDER)

    print_block("H1.1", "Renda média ponderada (R$) por escolaridade × grupo")
    print(piv.round(2).to_string())
    piv.reset_index().to_csv(
        out_dirs["tables"] / "H1_1_renda_escolaridade_grupo.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_heatmap_like(
        piv,
        "H1 — Renda média por escolaridade × grupo",
        out_dirs["figures"] / "H1_1_heatmap_renda_escolaridade.png",
        show=show,
        cmap=HEATMAP_CMAP,
        fmt=".0f",
    )

    if {"Homens Brancos", "Mulheres Pretas/Pardas"}.issubset(piv.columns):
        gap = ((piv["Mulheres Pretas/Pardas"] / piv["Homens Brancos"]) - 1) * 100

        print_block("H1.2", "Gap relativo (%) Mulheres Pretas/Pardas vs Homens Brancos por escolaridade")
        print(gap.round(1).to_string())
        gap.reset_index(name="gap_pct").to_csv(
            out_dirs["tables"] / "H1_2_gap_escolaridade.csv",
            index=False,
            encoding="utf-8-sig",
        )

        if len(gap):
            report.append(
                f"H1: o menor gap relativo entre Mulheres Pretas/Pardas e Homens Brancos aparece em '{gap.idxmin()}', com {gap.min():.1f}%."
            )

    annual = agg_wmean(df_table, ["ano", "grupo_h1"], value_col, weight_col)
    piv_a = annual.pivot(index="ano", columns="grupo_h1", values="valor")
    piv_a = reorder_columns_if_present(piv_a, GROUP_ORDER)

    if {"Mulheres Pretas/Pardas", "Homens Brancos"}.issubset(piv_a.columns):
        gap_a = ((piv_a["Mulheres Pretas/Pardas"] / piv_a["Homens Brancos"]) - 1) * 100

        print_block("H1.3", "Evolução anual do gap (%) Mulheres Pretas/Pardas vs Homens Brancos")
        print(gap_a.round(2).to_string())
        gap_a.reset_index(name="gap_pct").to_csv(
            out_dirs["tables"] / "H1_3_gap_anual.csv",
            index=False,
            encoding="utf-8-sig",
        )

        fig, ax = plt.subplots(figsize=(10.5, 5.2))
        ax.plot(gap_a.index.astype(int), gap_a.values, marker="o", linewidth=2.3, color="#4C8BB8")
        ax.axhline(0, linewidth=1)
        ax.set_title("H1 — Evolução anual do gap (%)\nMulheres Pretas/Pardas vs Homens Brancos")
        ax.set_xlabel("Ano")
        ax.set_ylabel("Gap (%)")
        ax.grid(True, alpha=0.4)
        fig.tight_layout()
        fig.savefig(out_dirs["figures"] / "H1_3_gap_anual.png", dpi=180)
        show_or_close(fig, show)

# =========================================================
# H2
# =========================================================
def bloco_h2(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    value_col: str,
    weight_col: str,
    show: bool,
    report: list[str],
) -> None:
    print_block("H2", "Retorno da escolaridade", HIPOTESES["H2"])

    tab = agg_wmean(df_table, ["escolaridade_desc", "grupo_h1"], value_col, weight_col)
    piv = tab.pivot(index="escolaridade_desc", columns="grupo_h1", values="valor")
    piv = reorder_index_if_present(piv, SCHOOL_ORDER)
    piv = reorder_columns_if_present(piv, GROUP_ORDER)

    returns = piv.pct_change(axis=0) * 100
    returns = returns.drop(index=["Sem instrução/Fundamental incompleto", "Médio incompleto"], errors="ignore")

    print_block("H2.1", "Retorno marginal entre níveis consecutivos de escolaridade (%)")
    print(returns.round(1).to_string())
    returns.reset_index().to_csv(
        out_dirs["tables"] / "H2_1_retorno_marginal.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_heatmap_like(
        returns,
        "H2 — Retorno marginal (%) entre níveis de escolaridade",
        out_dirs["figures"] / "H2_1_heatmap_retorno_marginal.png",
        show=show,
        cmap=HEATMAP_CMAP,
        fmt=".1f",
    )

    fig, ax = plt.subplots(figsize=(11, 5.5))
    x = returns.index.astype(str)
    for col in returns.columns:
        styled_line(ax, x, returns[col].values, col)

    ax.axhline(0, linewidth=1, color=GROUP_COLORS.get("Homens Brancos"))
    ax.set_title("H2 — Retorno marginal (%) entre níveis consecutivos de escolaridade")
    ax.set_xlabel("Escolaridade")
    ax.set_ylabel("Retorno marginal (%)")
    ax.tick_params(axis="x", rotation=25)
    ax.grid(True, alpha=0.4)
    ax.legend(frameon=True)
    fig.tight_layout()
    fig.savefig(out_dirs["figures"] / "H2_2_linhas_retorno_marginal.png", dpi=180)
    show_or_close(fig, show)

    if "Mulheres Pretas/Pardas" in returns.columns:
        s = returns["Mulheres Pretas/Pardas"].dropna()
        if not s.empty:
            report.append(
                f"H2: o maior retorno marginal para Mulheres Pretas/Pardas ocorre em '{s.idxmax()}', com {s.max():.1f}%."
            )

# =========================================================
# H3
# =========================================================
def bloco_h3(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    weight_col: str,
    show: bool,
    report: list[str],
) -> None:
    print_block("H3", "Estrutura ocupacional", HIPOTESES["H3"])

    if "ocup_h3" not in df_table.columns:
        print("[ERRO] H3 precisa de 'ocup_h3'.")
        return

    tab = wshare_table(df_table, ["grupo_h1"], "ocup_h3", weight_col)
    piv = tab.pivot(index="grupo_h1", columns="ocup_h3", values="pct").fillna(0)
    piv = piv.reindex(index=GROUP_ORDER)
    piv = piv[[c for c in H3_ORDER if c in piv.columns]]

    print_block("H3.1", "Distribuição ocupacional ponderada (%) por grupo")
    print(piv.round(2).to_string())
    piv.reset_index().to_csv(
        out_dirs["tables"] / "H3_1_distribuicao_ocupacional.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_stacked_bar(
        piv,
        "H3 — Distribuição ocupacional por grupo (ponderada, %)",
        out_dirs["figures"] / "H3_1_distribuicao_ocupacional.png",
        show=show,
    )

    if not piv.empty:
        major = piv.idxmax(axis=1)
        for grp, occ in major.items():
            report.append(f"H3: no grupo '{grp}', a maior concentração ocupacional está em '{occ}'.")

def bloco_h3_alta_escolaridade(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    weight_col: str,
    show: bool,
    report: list[str],
) -> None:
    if "ocup_h3" not in df_table.columns:
        print("[ERRO] H3.2 precisa de 'ocup_h3'.")
        return

    d = df_table[df_table["escolaridade_desc"].isin({"Superior completo", "Pós-graduação (ou equivalente)"})].copy()
    if d.empty:
        print("[ERRO] H3.2 ficou vazia.")
        return

    tab = wshare_table(d, ["grupo_h1"], "ocup_h3", weight_col)
    piv = tab.pivot(index="grupo_h1", columns="ocup_h3", values="pct").fillna(0)
    piv = piv.reindex(index=GROUP_ORDER)
    piv = piv[[c for c in H3_ORDER if c in piv.columns]]

    print_block("H3.2", "Distribuição ocupacional (%) entre altamente escolarizados")
    print(piv.round(2).to_string())
    piv.reset_index().to_csv(
        out_dirs["tables"] / "H3_2_distribuicao_ocupacional_alta_escolaridade.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_stacked_bar(
        piv,
        "H3.2 — Distribuição ocupacional (Superior completo + Pós)",
        out_dirs["figures"] / "H3_2_distribuicao_ocupacional_alta_escolaridade.png",
        show=show,
    )

    if "1. Diretores e gerentes" in piv.columns:
        s = piv["1. Diretores e gerentes"].dropna()
        if not s.empty:
            report.append(
                f"H3.2: entre altamente escolarizados, o maior percentual em 'Diretores e gerentes' está em '{s.idxmax()}', com {s.max():.2f}%."
            )

# =========================================================
# H4
# =========================================================
def bloco_formalizacao(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    weight_col: str,
    show: bool,
    formal_col: str,
    prefix: str,
    titulo_curto: str,
    report: list[str],
) -> None:
    if formal_col not in df_table.columns:
        print(f"[WARN] {prefix}: coluna {formal_col} ausente.")
        return

    d = df_table.copy()
    subtitulo = titulo_curto
    if formal_col == "formal_carteira":
        if "formal_carteira_escopo" not in d.columns:
            print(f"[WARN] {prefix}: formal_carteira_escopo ausente.")
            return
        d = d[d["formal_carteira_escopo"].eq(1)].copy()
        subtitulo = f"{titulo_curto} — apenas vínculos assalariados/passíveis de carteira"

    print_block(prefix, f"Formalização ({subtitulo})")

    tab = agg_weighted_share_binary(d, ["escolaridade_desc", "grupo_h1"], formal_col, weight_col, "pct_formal")
    piv = tab.pivot(index="escolaridade_desc", columns="grupo_h1", values="pct_formal")
    piv = reorder_index_if_present(piv, SCHOOL_ORDER)
    piv = reorder_columns_if_present(piv, GROUP_ORDER)

    print_block(f"{prefix}.1", "Formalização (%) por escolaridade × grupo")
    print(piv.round(1).to_string())
    piv.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_1_formalizacao_escolaridade.csv",
        index=False,
        encoding="utf-8-sig",
    )

    fig, ax = plt.subplots(figsize=(11.5, 5.8))
    for col in piv.columns:
        ax.plot(
            piv.index.astype(str),
            piv[col].values,
            marker="o",
            linewidth=2.2,
            label=col,
            color=GROUP_COLORS.get(str(col)),
        )
    ax.set_title(f"{prefix} — Formalização por escolaridade ({subtitulo})")
    ax.set_xlabel("Escolaridade")
    ax.set_ylabel("% formal")
    ax.tick_params(axis="x", rotation=25)
    ax.grid(True, alpha=0.35)
    ax.legend(frameon=True)
    fig.tight_layout()
    fig.savefig(out_dirs["figures"] / f"{prefix}_1_formalizacao_escolaridade.png", dpi=180)
    show_or_close(fig, show)

    annual = agg_weighted_share_binary(d, ["ano", "grupo_h1"], formal_col, weight_col, "pct_formal")
    piv_a = annual.pivot(index="ano", columns="grupo_h1", values="pct_formal")
    piv_a = reorder_columns_if_present(piv_a, GROUP_ORDER)

    print_block(f"{prefix}.2", "Formalização (%) por ano × grupo")
    print(piv_a.round(1).to_string())
    piv_a.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_2_formalizacao_anual.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_series_por_grupo_com_tendencia(
        piv_a,
        title=f"{prefix} — Formalização anual por grupo ({subtitulo})",
        outpath=out_dirs["figures"] / f"{prefix}_2_formalizacao_anual.png",
        show=show,
        y_label="% formal",
    )

    if "Mulheres Pretas/Pardas" in piv_a.columns and not piv_a.empty:
        report.append(
            f"{prefix}: média anual de formalização para Mulheres Pretas/Pardas varia de {piv_a['Mulheres Pretas/Pardas'].min():.1f}% a {piv_a['Mulheres Pretas/Pardas'].max():.1f}% ({subtitulo})."
        )

# =========================================================
# H5
# =========================================================
def bloco_qualidade_insercao(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    value_col: str,
    weight_col: str,
    show: bool,
    formal_col: str,
    prefix: str,
    titulo_curto: str,
    report: list[str],
) -> None:
    if "posicao_ocupacao_desc" not in df_table.columns:
        print(f"[WARN] {prefix}: posicao_ocupacao_desc ausente.")
        return
    if formal_col not in df_table.columns:
        print(f"[WARN] {prefix}: {formal_col} ausente.")
        return

    print_block(prefix, f"Estrutura ocupacional + renda ({titulo_curto})")

    d = df_table.copy()
    d["posicao_ocupacao_desc"] = pd.Categorical(d["posicao_ocupacao_desc"], categories=POS_ORDER, ordered=True)

    tab_pos = wshare_table(d, ["grupo_h1"], "posicao_ocupacao_desc", weight_col)
    piv_pos = tab_pos.pivot(index="grupo_h1", columns="posicao_ocupacao_desc", values="pct").fillna(0)
    piv_pos = piv_pos.reindex(index=GROUP_ORDER)
    piv_pos = piv_pos[[c for c in POS_ORDER if c in piv_pos.columns]]

    print_block(f"{prefix}.1", "Distribuição ponderada (%) por posição na ocupação")
    print(piv_pos.round(2).to_string())
    piv_pos.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_1_distribuicao_posicao.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_stacked_bar(
        piv_pos,
        f"{prefix} — Distribuição por posição na ocupação ({titulo_curto})",
        out_dirs["figures"] / f"{prefix}_1_distribuicao_posicao.png",
        show=show,
    )

    d_r = d.dropna(subset=["posicao_ocupacao_desc", "grupo_h1", value_col, weight_col]).copy()
    d_r = d_r.sort_values(by=["posicao_ocupacao_desc", "grupo_h1"]).copy()
    rows = []
    for (pos, grp), g in d_r.groupby(["posicao_ocupacao_desc", "grupo_h1"], dropna=False, observed=False):
        rows.append({
            "posicao_ocupacao_desc": pos,
            "grupo_h1": grp,
            "valor": wmean(g[value_col], g[weight_col]),
        })
    tab_r = pd.DataFrame(rows)

    if not tab_r.empty:
        piv_r = tab_r.pivot(index="posicao_ocupacao_desc", columns="grupo_h1", values="valor")
        piv_r = reorder_columns_if_present(piv_r, GROUP_ORDER)

        ordered_idx = [c for c in POS_ORDER if c in piv_r.index]
        other_idx = [c for c in piv_r.index if c not in ordered_idx]
        piv_r = piv_r.loc[ordered_idx + other_idx]

        print_block(f"{prefix}.2", "Renda média ponderada (R$) por posição na ocupação × grupo")
        print(piv_r.round(2).to_string())
        piv_r.reset_index().to_csv(
            out_dirs["tables"] / f"{prefix}_2_renda_posicao.csv",
            index=False,
            encoding="utf-8-sig",
        )

        plot_heatmap_like(
            piv_r,
            f"{prefix} — Renda média por posição na ocupação × grupo ({titulo_curto})",
            out_dirs["figures"] / f"{prefix}_2_heatmap_renda_posicao.png",
            show=show,
            cmap=HEATMAP_CMAP,
            fmt=".0f",
        )

    if formal_col == "formal_carteira":
        d_assal = d[d["posicao_ocupacao_desc"].astype(str).isin([
            "Privado com carteira", "Privado sem carteira",
            "Doméstico com carteira", "Doméstico sem carteira",
            "Público com carteira", "Público sem carteira",
            "Militar/estatutário",
        ])].copy()

        tab_inf = agg_weighted_share_binary(d_assal, ["grupo_h1"], formal_col, weight_col, "pct_formal")
        tab_inf["pct_informal"] = 100 - tab_inf["pct_formal"]
        tab_inf = tab_inf.set_index("grupo_h1").reindex(GROUP_ORDER)

        print_block(f"{prefix}.3", "Informalidade (%) no escopo assalariado elegível à carteira")
        print(tab_inf["pct_informal"].round(2).to_string())
        tab_inf.reset_index()[["grupo_h1", "pct_informal"]].to_csv(
            out_dirs["tables"] / f"{prefix}_3_informalidade_escopo_assalariado.csv",
            index=False,
            encoding="utf-8-sig",
        )

        s = tab_inf["pct_informal"].dropna()
        if not s.empty:
            report.append(
                f"{prefix}: no escopo assalariado elegível à carteira, a maior informalidade está em '{s.idxmax()}', com {s.max():.2f}%."
            )
    elif "Conta própria" in d["posicao_ocupacao_desc"].astype(str).unique():
        d_cp = d[d["posicao_ocupacao_desc"].astype(str) == "Conta própria"].copy()
        tab_inf = agg_weighted_share_binary(d_cp, ["grupo_h1"], formal_col, weight_col, "pct_formal")
        tab_inf["pct_informal"] = 100 - tab_inf["pct_formal"]
        tab_inf = tab_inf.set_index("grupo_h1").reindex(GROUP_ORDER)

        print_block(f"{prefix}.3", "Informalidade (%) dentro de Conta própria")
        print(tab_inf["pct_informal"].round(2).to_string())
        tab_inf.reset_index()[["grupo_h1", "pct_informal"]].to_csv(
            out_dirs["tables"] / f"{prefix}_3_informalidade_conta_propria.csv",
            index=False,
            encoding="utf-8-sig",
        )

        s = tab_inf["pct_informal"].dropna()
        if not s.empty:
            report.append(
                f"{prefix}: dentro de Conta própria, a maior informalidade está em '{s.idxmax()}', com {s.max():.2f}%."
            )

# =========================================================
# H6
# =========================================================
def bloco_h6_jovens(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    value_col: str,
    weight_col: str,
    show: bool,
    report: list[str],
) -> None:
    print_block("H6", "Jovens no mercado de trabalho", HIPOTESES["H6"])

    d = df_table[(df_table["idade"] >= 18) & (df_table["idade"] <= 29)].copy()
    if d.empty:
        print("[ERRO] H6 vazia após recorte etário.")
        return

    tab_r = agg_wmean(d, ["escolaridade_desc", "grupo_h1"], value_col, weight_col)
    piv_r = tab_r.pivot(index="escolaridade_desc", columns="grupo_h1", values="valor")
    piv_r = reorder_index_if_present(piv_r, SCHOOL_ORDER)
    piv_r = reorder_columns_if_present(piv_r, GROUP_ORDER)

    print_block("H6.1", "Renda média ponderada (R$) entre jovens por escolaridade × grupo")
    print(piv_r.round(2).to_string())
    piv_r.reset_index().to_csv(
        out_dirs["tables"] / "H6_1_renda_jovens.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_heatmap_like(
        piv_r,
        "H6 — Renda média entre jovens por escolaridade × grupo",
        out_dirs["figures"] / "H6_1_heatmap_renda_jovens.png",
        show=show,
        cmap=HEATMAP_CMAP,
        fmt=".0f",
    )

    if {"Homens Brancos", "Mulheres Pretas/Pardas"}.issubset(piv_r.columns):
        gap = ((piv_r["Mulheres Pretas/Pardas"] / piv_r["Homens Brancos"]) - 1) * 100
        if not gap.dropna().empty:
            report.append(
                f"H6: entre jovens, o pior gap para Mulheres Pretas/Pardas ocorre em '{gap.idxmin()}', com {gap.min():.1f}%."
            )

# =========================================================
# H7
# =========================================================
_H7_PRINTED = False
def build_h7_mask(df: pd.DataFrame, mode: str) -> tuple[pd.Series, str, str]:
    detalhe = (
        df["cbo_detalhe_label"].astype(str).str.lower()
        if "cbo_detalhe_label" in df.columns
        else pd.Series(False, index=df.index)
    )
    grupo = df["ocup_h3"].astype(str) if "ocup_h3" in df.columns else pd.Series("", index=df.index)

    if mode == "diretoria":
        return detalhe.str.contains("diretor", na=False), "Diretoria detalhada (CBO)", "H7_A"
    if mode == "gerentes":
        return detalhe.str.contains("gerente", na=False), "Gerentes (CBO detalhado)", "H7_B"
    return grupo.str.startswith("1.", na=False), "Diretores + gerentes (grande grupo)", "H7_C"

def bloco_h7_modo(
    df_table: pd.DataFrame,
    out_dirs: dict[str, Path],
    weight_col: str,
    show: bool,
    mode: str,
    report: list[str],
) -> None:
    mask, titulo, prefix = build_h7_mask(df_table, mode)
    d = df_table[mask].copy()

    if d.empty:
        print_block(prefix, f"Cargos de liderança — {titulo}")
        print("[WARN] Recorte vazio.")
        return

    global _H7_PRINTED
    if not _H7_PRINTED:
        print_block("H7", "Cargos de liderança", HIPOTESES["H7"])
        _H7_PRINTED = True
    print_block(prefix, titulo)

    d = d.sort_values(by=["ano", "grupo_h1"]).copy()
    rows = []
    for (ano, grp), g in d.groupby(["ano", "grupo_h1"], dropna=False, observed=False):
        rows.append({
            "ano": ano,
            "grupo_h1": grp,
            "cadeiras_est": precise_sum(g[weight_col]),
        })
    tab = (
        pd.DataFrame(rows)
        .pivot(index="ano", columns="grupo_h1", values="cadeiras_est")
        .sort_index()
    )
    tab = reorder_columns_if_present(tab, GROUP_ORDER)

    print_block(f"{prefix}.1", "Nº estimado de cadeiras por ano × grupo")
    print(tab.round(0).astype("Int64").to_string())
    tab.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_1_cadeiras_por_ano.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_series_por_grupo_com_tendencia(
        tab,
        f"{prefix} — Nº estimado de cadeiras por ano × grupo ({titulo})",
        out_dirs["figures"] / f"{prefix}_1_cadeiras_por_ano.png",
        show=show,
        y_label="Nº estimado de ocupados (peso)",
    )

    total = tab.apply(lambda col: precise_sum(col)).sort_values(ascending=False)

    print_block(f"{prefix}.2", "Total acumulado de cadeiras no período")
    print(total.round(0).astype("Int64").to_string())
    total.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_2_total_periodo.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_bar_rank(
        total,
        f"{prefix} — Total acumulado de cadeiras no período ({titulo})",
        out_dirs["figures"] / f"{prefix}_2_total_periodo.png",
        show=show,
        ylabel="Nº estimado de ocupados (peso)",
        fmt="{:.0f}",
    )

    row_sums = tab.apply(lambda row: precise_sum(row), axis=1)
    share = tab.div(row_sums, axis=0) * 100
    share = reorder_columns_if_present(share, GROUP_ORDER)

    print_block(f"{prefix}.3", "Participação percentual por grupo dentro do total anual de cadeiras")
    print(share.round(2).to_string())
    share.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_3_participacao_pct_anual.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_stacked_bar(
        share,
        f"{prefix} — Participação percentual anual por grupo ({titulo})",
        out_dirs["figures"] / f"{prefix}_3_participacao_pct_anual.png",
        show=show,
        ylabel="Participação no total anual (%)",
    )

    df_table = df_table.sort_values(by=["ano", "grupo_h1"]).copy()
    pop_rows = []
    for (ano, grp), g in df_table.groupby(["ano", "grupo_h1"], dropna=False, observed=False):
        pop_rows.append({
            "ano": ano,
            "grupo_h1": grp,
            "ocupados_est": precise_sum(g[weight_col]),
        })
    pop_share = (
        pd.DataFrame(pop_rows)
        .pivot(index="ano", columns="grupo_h1", values="ocupados_est")
        .sort_index()
    )
    pop_share = reorder_columns_if_present(pop_share, GROUP_ORDER)
    pop_row_sums = pop_share.apply(lambda row: precise_sum(row), axis=1)
    pop_share = pop_share.div(pop_row_sums, axis=0) * 100

    repr_ratio = share.div(pop_share)
    repr_ratio = reorder_columns_if_present(repr_ratio, GROUP_ORDER)

    print_block(f"{prefix}.4", "Razão de representação relativa (cadeiras / ocupados)")
    print(repr_ratio.round(3).to_string())
    repr_ratio.reset_index().to_csv(
        out_dirs["tables"] / f"{prefix}_4_razao_representacao_anual.csv",
        index=False,
        encoding="utf-8-sig",
    )

    plot_heatmap_like(
        repr_ratio.T,
        f"{prefix} — Razão de representação relativa por ano × grupo ({titulo})",
        out_dirs["figures"] / f"{prefix}_4_heatmap_razao_representacao.png",
        show=show,
        cmap=HEATMAP_CMAP,
        fmt=".2f",
    )

    if not total.empty:
        report.append(
            f"{prefix}: no recorte '{titulo}', o maior total acumulado de cadeiras no período está em '{total.idxmax()}', com {total.max():.0f}."
        )

    if not repr_ratio.empty:
        ratio_mean = repr_ratio.mean().dropna()
        if not ratio_mean.empty:
            report.append(
                f"{prefix}: na razão média de representação relativa, o grupo mais sobre-representado é '{ratio_mean.idxmax()}' ({ratio_mean.max():.2f}) e o mais sub-representado é '{ratio_mean.idxmin()}' ({ratio_mean.min():.2f})."
            )

# =========================================================
# MAIN
# =========================================================
def main() -> None:
    args = parse_args()
    out_root = Path(args.out)
    out_dirs = ensure_dirs(out_root)
    report_lines: list[str] = []

    print_block("BASE", "Carregando base consolidada")

    read_cols = None
    print(f"[INFO] Colunas lidas do parquet: {read_cols}")

    df_raw = pd.read_csv(
        args.input,
        low_memory=False,
        dtype_backend="pyarrow"
    )

    df = prepare_base(
        df_raw,
        value_col=args.value_col,
        weight_col=args.weight_col,
        idade_min=args.idade_min,
        idade_max=args.idade_max,
    )

    print(f"[INFO] Base: {args.input}")
    print(f"[INFO] Linhas após preparação: {len(df):,}")
    print(f"[INFO] Valor principal: {args.value_col}")
    print(f"[INFO] Peso: {args.weight_col}")
    print(f"[INFO] Diretório de saída: {out_root}")

    df_plot = df
    if args.winsorize and args.value_col in df.columns:
        p = float(args.winsorize_q)
        cap = df[args.value_col].quantile(p)
        df_plot = df.copy(deep=False)
        df_plot = df_plot.assign(**{args.value_col: np.minimum(df_plot[args.value_col], cap)})
        print(f"[INFO] Winsorização visual ativa em q={p:.2f}: teto = {fmt_money(cap)}")

    bloco_h1(df_plot, out_dirs, args.value_col, args.weight_col, args.show, report_lines)
    bloco_h2(df_plot, out_dirs, args.value_col, args.weight_col, args.show, report_lines)
    bloco_h3(df, out_dirs, args.weight_col, args.show, report_lines)
    bloco_h3_alta_escolaridade(df, out_dirs, args.weight_col, args.show, report_lines)

    print_block("H4", "Formalização", HIPOTESES["H4"])
    bloco_formalizacao(
        df,
        out_dirs,
        args.weight_col,
        args.show,
        formal_col="formal_ampliado",
        prefix="H4_A",
        titulo_curto="formalização ampliada",
        report=report_lines,
    )
    bloco_formalizacao(
        df,
        out_dirs,
        args.weight_col,
        args.show,
        formal_col="formal_carteira",
        prefix="H4_B",
        titulo_curto="formalização por carteira",
        report=report_lines,
    )

    print_block("H5", "Estrutura ocupacional + renda", HIPOTESES["H5"])
    bloco_qualidade_insercao(
        df_plot,
        out_dirs,
        args.value_col,
        args.weight_col,
        args.show,
        formal_col="formal_ampliado",
        prefix="H5_A",
        titulo_curto="formalização ampliada",
        report=report_lines,
    )
    bloco_qualidade_insercao(
        df_plot,
        out_dirs,
        args.value_col,
        args.weight_col,
        args.show,
        formal_col="formal_carteira",
        prefix="H5_B",
        titulo_curto="formalização por carteira",
        report=report_lines,
    )

    bloco_h6_jovens(df_plot, out_dirs, args.value_col, args.weight_col, args.show, report_lines)

    bloco_h7_modo(df, out_dirs, args.weight_col, args.show, mode="diretoria", report=report_lines)
    bloco_h7_modo(df, out_dirs, args.weight_col, args.show, mode="gerentes", report=report_lines)
    bloco_h7_modo(df, out_dirs, args.weight_col, args.show, mode="diretores_gerentes", report=report_lines)

    report_path = out_dirs["reports"] / "relatorio_principais_achados.txt"
    report_header = [
        "Relatório de principais achados — PNAD-C / EDA",
        "Autor: Alberto Nagem — Universidade Presbiteriana Mackenzie",
        f"Base: {args.input}",
        f"Linhas após preparação: {len(df):,}",
        "",
    ]
    write_report(report_path, report_header + report_lines)

    print("\n[OK] EDA concluída - Autor: Alberto Nagem")
    print(f"Tabelas: {out_dirs['tables']}")
    print(f"Figuras: {out_dirs['figures']}")
    print(f"Relatório: {report_path}")

if __name__ == "__main__":
    main()