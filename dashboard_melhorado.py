# -*- coding: utf-8 -*-
"""
Métricas de Veículos (v3.7)
- 'Nome do Veículo' como Dropdown multi-seleção (f_sites)
- Coluna 'Valor Planejado' ANTES de 'Valor Pago'
- 'Valor' renomeado para 'Valor Pago' em toda a aplicação
- 'Saldo' = Valor Planejado - Valor Pago
- Filtro 'Somente Valor Pago ≠ 0'
- Exportar Excel/CSV e PDF incluem Valor Planejado, Valor Pago e Saldo
- Meses Jul/Ago/Set + Média Trimestral; Top 10 por Média; Tabela contínua
"""

import os
import time
import unicodedata
from io import BytesIO

import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import Dash, dcc, html, dash_table
from dash import Input, Output, State
from dash.dash_table.Format import Format, Group, Scheme

# ========= FONTE DE DADOS =========
EXCEL_PATH = "Recadastramento (respostas).xlsx"   # fallback local (dev)
SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL")      # defina no Render (CSV público)

def _url_with_cache_bust(url: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_t={int(time.time())}"

# ========= HELPERS =========
def _normalize(colname: str) -> str:
    s = str(colname).strip().replace("\n", " ")
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    s = s.lower()
    for ch in ["  ", "   "]:
        s = s.replace(ch, " ")
    s = s.replace(" ", "_")
    return s

def clean_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace(r"[^0-9\-,\.]", "", regex=True)
    s = s.str.replace(",", ".", regex=False)
    s = s.str.replace(r"(?<=\d)\.(?=\d{3}(?:\.|$))", "", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def _find_first(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _find_by_tokens(df: pd.DataFrame, tokens: list[str]) -> str | None:
    for c in df.columns:
        if all(tok in c for tok in tokens):
            return c
    return None

def _resolve_views(df: pd.DataFrame, mes: str) -> str | None:
    base_aliases = ["visualizacoes", "vizualizacoes", "views", "pageviews", "page_views", "pageview"]
    aliases = [f"visualizacoes_{mes}", f"vizualizacoes_{mes}",
               f"total_de_visualizacoes_{mes}", f"total_de_vizualizacoes_{mes}"]
    for a in base_aliases:
        aliases += [f"{a}_{mes}", f"{a}_de_{mes}", f"{a}__{mes}", f"{mes}_{a}"]
    hit = _find_first(df, aliases)
    if hit: return hit
    for a in base_aliases:
        hit = _find_by_tokens(df, [a, mes])
        if hit: return hit
    abrevs_map = {"julho": ["jul"], "agosto": ["ago"], "setembro": ["set", "sep"]}
    for ab in abrevs_map.get(mes, []):
        for a in base_aliases:
            hit = _find_by_tokens(df, [a, ab])
            if hit: return hit
    return None

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    original_cols = list(df.columns)
    df.columns = [_normalize(c) for c in df.columns]
    print("[prepare] Colunas normalizadas:", df.columns.tolist())

    # Nome do veículo
    if "nome_fantasia" not in df.columns:
        cand = _find_first(df, ["nome_do_veiculo", "nomedoveiculo", "nome", "nome_site", "site"])
        if not cand:
            cand = _find_by_tokens(df, ["nome", "veiculo"]) or _find_by_tokens(df, ["nome", "site"])
        df["nome_fantasia"] = df[cand].astype(str) if cand else ""
    df["nome_do_veiculo"] = df["nome_fantasia"].astype(str)

    # URL
    if "url" not in df.columns:
        cand = _find_first(df, ["url", "url_ativa_do_veiculo.", "url_ativa_do_veiculo", "url_do_site", "link"])
        df["url"] = df[cand].astype(str) if cand else ""

    # Campos de filtro
    for c in ["cidade", "categoria", "status"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        else:
            df[c] = "Não informado"
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper()

    # Motivo
    motivo_aliases = ["motivo", "motivo_da_reprovacao", "motivo_de_reprovacao",
                      "motivo_reprovacao", "motivo_reprova", "motivo_reprov"]
    mot = _find_first(df, motivo_aliases) or _find_by_tokens(df, ["motivo", "reprov"])
    if mot and mot != "motivo":
        df["motivo"] = df[mot].astype(str)
    elif "motivo" not in df.columns:
        df["motivo"] = ""
    df["motivo"] = df["motivo"].fillna("").astype(str).str.strip()

    # Visualizações por mês (Jul/Ago/Set)
    for mes in ["julho", "agosto", "setembro"]:
        col = _resolve_views(df, mes)
        if col:
            df[f"visualizacoes_{mes}"] = clean_numeric(df[col])
        else:
            print(f"[prepare] NÃO encontrei coluna de visualizações de {mes.upper()} nas colunas originais:", original_cols)
            df[f"visualizacoes_{mes}"] = 0.0

    df["total_visualizacoes"] = (
        df.get("visualizacoes_julho", 0.0) + df.get("visualizacoes_agosto", 0.0) + df.get("visualizacoes_setembro", 0.0)
    )
    df["media_trimestral"] = df[["visualizacoes_julho", "visualizacoes_agosto", "visualizacoes_setembro"]].mean(axis=1)

    return df

def load_data() -> pd.DataFrame:
    if SHEETS_CSV_URL:
        try:
            url = _url_with_cache_bust(SHEETS_CSV_URL)
            raw = pd.read_csv(url)
            print("[load_data] Sheets OK. Linhas:", len(raw), "Colunas originais:", list(raw.columns))
            return _prepare_df(raw)
        except Exception as e:
            print("[load_data] Falha lendo SHEETS_CSV_URL:", e)
    try:
        base = pd.read_excel(EXCEL_PATH)
        print("[load_data] Excel local OK. Linhas:", len(base))
        return _prepare_df(base)
    except Exception as e:
        print("[load_data] Excel local indisponível e Sheets falhou:", e)
        cols = [
            "nome_fantasia","nome_do_veiculo","cidade","status","motivo","categoria",
            "visualizacoes_julho","visualizacoes_agosto","visualizacoes_setembro",
            "total_visualizacoes","media_trimestral","url"
        ]
        empty = pd.DataFrame(columns=cols)
        return _prepare_df(empty)

# Base inicial p/ filtros
DF_BASE = load_data()

# ========= TEMA/CORES =========
THEME_COLORS = {
    "light": {"font":"#0F172A","muted":"#64748B","grid":"#E9EDF5","paper":"rgba(0,0,0,0)","plot":"rgba(0,0,0,0)",
              "colorway":["#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA"],"template":"plotly_white"},
    "dark":  {"font":"#E6ECFF","muted":"#93A3BE","grid":"#22304A","paper":"rgba(0,0,0,0)","plot":"rgba(0,0,0,0)",
              "colorway":["#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD"],"template":"plotly_dark"},
}
EXTENDED_SEQ = {
    "light": ["#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA","#10B981","#F43F5E","#8B5CF6","#14B8A6","#EAB308","#0EA5E9"],
    "dark":  ["#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD","#4ADE80","#FB7185","#A78BFA","#2DD4BF","#FACC15","#38BDF8"],
}
def get_sequence(theme: str, n: int):
    seq = EXTENDED_SEQ.get(theme, EXTENDED_SEQ["light"])
    if n <= len(seq): return seq[:n]
    times = (n // len(seq)) + 1
    return (seq * times)[:n]

def style_fig(fig, theme="light"):
    c = THEME_COLORS[theme]
    fig.update_layout(
        template=c["template"], colorway=c["colorway"],
        paper_bgcolor=c["paper"], plot_bgcolor=c["plot"],
        font=dict(color=c["font"], size=13),
        title=dict(font=dict(color=c["font"], size=16)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=c["font"])),
        margin=dict(l=12, r=12, t=48, b=12),
        uniformtext_minsize=12, uniformtext_mode="hide",
        hoverlabel=dict(bgcolor="rgba(15,23,42,0.95)" if theme=="dark" else "#ffffff",
                        font_color=c["font"], bordercolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(gridcolor=c["grid"], zerolinecolor=c["grid"],
                     tickfont=dict(color=c["font"]), title_font=dict(color=c["font"]))
    fig.update_yaxes(gridcolor=c["grid"], zerolinecolor=c["grid"],
                     tickfont=dict(color=c["font"]), title_font=dict(color=c["font"]))
    fig.update_traces(selector=dict(type="bar"),
                      textfont_color=c["font"], marker_line_width=0,
                      opacity=0.95 if theme=="dark" else 1.0)
    return fig

# ========= APP =========
app = Dash(__name__)
server = app.server

def kpi_card(kpi_id: str, label: str):
    return html.Div(className="card kpi", children=[
        html.P(label, className="kpi-title"),
        html.H2(id=kpi_id, className="kpi-value"),
    ])

# claro por padrão
app.layout = html.Div(className="light", id="root", children=[
    # Armazenamento de valores (por sessão)
    dcc.Store(id="store_valores", storage_type="session"),  # {nome: {"valor_pago": x, "valor_planejado": y}}
    html.Div(className="container", children=[
        # Navbar
        html.Div(className="navbar", children=[
            html.Div(className="brand", children=[
                html.Div("📊", style={"fontSize": "20px"}),
                html.H1("Métricas de Veículos"),
                html.Span("v3.7", className="badge"),
            ]),
            html.Div(className="actions", children=[
                dcc.RadioItems(
                    id="theme-toggle",
                    options=[{"label":"Claro","value":"light"},{"label":"Escuro","value":"dark"}],
                    value="light", inline=True,
                    inputStyle={"marginRight":"6px","marginLeft":"10px"},
                    style={"marginRight":"8px"},
                ),
                html.Button("Atualizar dados", id="btn-reload", n_clicks=0, className="btn ghost"),
                html.Button("Exportar Excel", id="btn-export-excel", n_clicks=0, className="btn"),
                html.Button("Exportar PDF", id="btn-export-pdf", n_clicks=0, className="btn"),
                dcc.Download(id="download_excel"),
                dcc.Download(id="download_pdf"),
            ]),
        ]),

        # Filtros
        html.Div(className="panel", children=[
            html.Div(className="filters", children=[
                html.Div(children=[
                    html.Div("Cidade", className="label"),
                    dcc.Dropdown(
                        id="f_cidade",
                        options=[{"label": c, "value": c} for c in sorted(DF_BASE["cidade"].dropna().unique())] if "cidade" in DF_BASE else [],
                        multi=True, placeholder="Selecione cidades…",
                    ),
                ]),
                html.Div(children=[
                    html.Div("Status", className="label"),
                    dcc.Dropdown(
                        id="f_status",
                        options=[{"label": s, "value": s} for s in sorted(DF_BASE["status"].dropna().unique())] if "status" in DF_BASE else [],
                        multi=True, placeholder="Selecione status…",
                    ),
                ]),
                html.Div(children=[
                    html.Div("Categoria", className="label"),
                    dcc.Dropdown(
                        id="f_categoria",
                        options=[{"label": c, "value": c} for c in sorted(DF_BASE["categoria"].dropna().unique())] if "categoria" in DF_BASE else [],
                        multi=True, placeholder="Selecione categorias…",
                    ),
                ]),
                html.Div(children=[
                    html.Div("Nome do Veículo (selecione um ou vários)", className="label"),
                    dcc.Dropdown(
                        id="f_sites",
                        options=[{"label": n, "value": n} for n in sorted(DF_BASE["nome_do_veiculo"].dropna().unique())] if "nome_do_veiculo" in DF_BASE else [],
                        multi=True, placeholder="Selecione sites…", clearable=True, searchable=True,
                    ),
                ]),
                html.Div(children=[
                    html.Div("Filtro de Valor Pago", className="label"),
                    dcc.Checklist(
                        id="f_valor_nzero",
                        options=[{"label": "Somente Valor Pago ≠ 0", "value": "nz"}],
                        value=[],
                        inline=True,
                        inputStyle={"marginRight":"6px","marginLeft":"10px"},
                    ),
                ]),
                html.Div(children=[
                    html.Div("Ordenação dos gráficos", className="label"),
                    dcc.RadioItems(
                        id="sort-order",
                        options=[{"label":"Decrescente","value":"desc"},{"label":"Crescente","value":"asc"}],
                        value="desc", inline=True,
                        inputStyle={"marginRight":"6px","marginLeft":"10px"},
                    ),
                ]),
            ]),
        ]),

        # KPIs
        html.Div(className="kpis", children=[
            kpi_card("kpi_total", "Total de Veículos"),
            kpi_card("kpi_aprov", "Aprovados"),
            kpi_card("kpi_reprov", "Reprovados"),
            kpi_card("kpi_cidades", "Cidades"),
        ]),

        # Gráficos
        html.Div(className="grid-2", children=[
            html.Div(className="card", children=[dcc.Graph(id="g_status", config={"displayModeBar": False})]),
            html.Div(className="card", children=[dcc.Graph(id="g_top_cidades", config={"displayModeBar": False})]),
        ]),
        html.Div(className="grid-2", children=[
            html.Div(className="card", children=[dcc.Graph(id="g_meses", config={"displayModeBar": False})]),
            html.Div(className="card", children=[dcc.Graph(id="g_top_sites", config={"displayModeBar": False})]),
        ]),

        # Tabela (lista contínua)
        html.Div(className="panel", children=[
            html.Div("Dados detalhados", className="label"),
            html.Div(className="card", children=[
                dash_table.DataTable(
                    id="tbl",
                    page_action="none",
                    sort_action="native",
                    filter_action="native",
                    fixed_rows={"headers": True},
                    style_table={"overflowX":"auto","minWidth":"100%","maxHeight":"70vh","overflowY":"auto"},
                    style_cell={"padding":"10px","textAlign":"left","border":"0","whiteSpace":"normal","height":"auto"},
                    style_header={"fontWeight":"700","border":"0"},
                    style_cell_conditional=[
                        {"if": {"column_id": "nome_do_veiculo"}, "min
