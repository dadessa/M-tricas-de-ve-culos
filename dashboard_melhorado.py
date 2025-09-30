# -*- coding: utf-8 -*-
"""
Métricas de Veículos (v2.9)
- Dados detalhados em lista contínua (sem paginação) com rolagem vertical
- Dados detalhados: Nome do Veículo, Cidade, Status, Motivo, Média Trimestral
- Exportar PDF ajustado para caber (gráficos + tabela)
- Exportar Excel via dcc.send_data_frame (compatível) + fallback CSV
- Atualizar dados do Google Sheets (CSV público) com cache-busting
- Tema claro/escuro (claro por padrão), barras multicolor, REPROVADO vermelho
- Coluna 'motivo' na tabela
Requisitos extras para PDF com gráficos: kaleido
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
    s = s.str.replace(r"[^0-9\-,\.]", "", regex=True)                 # mantém dígitos/-,./,
    s = s.str.replace(",", ".", regex=False)                          # vírgula -> ponto
    s = s.str.replace(r"(?<=\d)\.(?=\d{3}(?:\.|$))", "", regex=True)  # remove pontos de milhar
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
    abrevs = {"junho": ["jun"], "julho": ["jul"], "agosto": ["ago"]}[mes]
    for ab in abrevs:
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

    # Motivo (da reprovação)
    motivo_aliases = ["motivo", "motivo_da_reprovacao", "motivo_de_reprovacao",
                      "motivo_reprovacao", "motivo_reprova", "motivo_reprov"]
    mot = _find_first(df, motivo_aliases) or _find_by_tokens(df, ["motivo", "reprov"])
    if mot and mot != "motivo":
        df["motivo"] = df[mot].astype(str)
    elif "motivo" not in df.columns:
        df["motivo"] = ""
    df["motivo"] = df["motivo"].fillna("").astype(str).str.strip()

    # Visualizações por mês
    for mes in ["junho", "julho", "agosto"]:
        col = _resolve_views(df, mes)
        if col:
            df[f"visualizacoes_{mes}"] = clean_numeric(df[col])
        else:
            print(f"[prepare] NÃO encontrei coluna de visualizações de {mes.upper()} nas colunas originais:", original_cols)
            df[f"visualizacoes_{mes}"] = 0.0

    df["total_visualizacoes"] = (
        df["visualizacoes_junho"] + df["visualizacoes_julho"] + df["visualizacoes_agosto"]
    )
    # média trimestral (Jun/Jul/Ago)
    df["media_trimestral"] = (
        df[["visualizacoes_junho", "visualizacoes_julho", "visualizacoes_agosto"]].mean(axis=1)
    )

    print("[prepare] Usando views ->",
          "junho:", _resolve_views(df, "junho"),
          "| julho:", _resolve_views(df, "julho"),
          "| agosto:", _resolve_views(df, "agosto"))
    print("[prepare] Somas -> jun:", df["visualizacoes_junho"].sum(),
          "jul:", df["visualizacoes_julho"].sum(),
          "ago:", df["visualizacoes_agosto"].sum())

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
            "visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto",
            "total_visualizacoes","media_trimestral","url"
        ]
        empty = pd.DataFrame(columns=cols)
        return _prepare_df(empty)

# Base inicial p/ filtros
DF_BASE = load_data()

# ========= TEMA/CORES =========
THEME_COLORS = {
    "light": {
        "font": "#0F172A", "muted": "#64748B", "grid": "#E9EDF5",
        "paper": "rgba(0,0,0,0)", "plot": "rgba(0,0,0,0)",
        "colorway": ["#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA"],
        "template": "plotly_white",
    },
    "dark": {
        "font": "#E6ECFF", "muted": "#93A3BE", "grid": "#22304A",
        "paper": "rgba(0,0,0,0)", "plot": "rgba(0,0,0,0)",
        "colorway": ["#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD"],
        "template": "plotly_dark",
    },
}
EXTENDED_SEQ = {
    "light": ["#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA",
              "#10B981","#F43F5E","#8B5CF6","#14B8A6","#EAB308","#0EA5E9"],
    "dark":  ["#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD",
              "#4ADE80","#FB7185","#A78BFA","#2DD4BF","#FACC15","#38BDF8"],
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
        hoverlabel=dict(
            bgcolor="rgba(15,23,42,0.95)" if theme == "dark" else "#ffffff",
            font_color=c["font"], bordercolor="rgba(0,0,0,0)",
        ),
    )
    fig.update_xaxes(gridcolor=c["grid"], zerolinecolor=c["grid"],
                     tickfont=dict(color=c["font"]), title_font=dict(color=c["font"]))
    fig.update_yaxes(gridcolor=c["grid"], zerolinecolor=c["grid"],
                     tickfont=dict(color=c["font"]), title_font=dict(color=c["font"]))
    fig.update_traces(selector=dict(type="bar"),
                      textfont_color=c["font"], marker_line_width=0,
                      opacity=0.95 if theme == "dark" else 1.0)
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
    html.Div(className="container", children=[
        # Navbar
        html.Div(className="navbar", children=[
            html.Div(className="brand", children=[
                html.Div("📊", style={"fontSize": "20px"}),
                html.H1("Métricas de Veículos"),
                html.Span("v2.9", className="badge"),
            ]),
            html.Div(className="actions", children=[
                dcc.RadioItems(
                    id="theme-toggle",
                    options=[{"label": "Claro", "value": "light"},
                             {"label": "Escuro", "value": "dark"}],
                    value="light", inline=True,
                    inputStyle={"marginRight": "6px", "marginLeft": "10px"},
                    style={"marginRight": "8px"},
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
                    html.Div("Buscar por nome do site", className="label"),
                    dcc.Input(id="f_busca", type="text", placeholder="Digite parte do nome…", debounce=True),
                ]),
                html.Div(children=[
                    html.Div("Ordenação dos gráficos", className="label"),
                    dcc.RadioItems(
                        id="sort-order",
                        options=[{"label": "Decrescente", "value": "desc"},
                                 {"label": "Crescente", "value": "asc"}],
                        value="desc", inline=True,
                        inputStyle={"marginRight": "6px", "marginLeft": "10px"},
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

        # Tabela (lista contínua com rolagem, sem paginação)
        html.Div(className="panel", children=[
            html.Div("Dados detalhados", className="label"),
            html.Div(className="card", children=[
                dash_table.DataTable(
                    id="tbl",
                    # <<< mudanças principais >>>
                    page_action="none",                     # desativa paginação (lista contínua)
                    # page_size removido (não é usado quando page_action="none")
                    sort_action="native",
                    filter_action="native",
                    fixed_rows={"headers": True},
                    style_table={
                        "overflowX": "auto",
                        "minWidth": "100%",
                        "maxHeight": "70vh",                 # altura da área da tabela
                        "overflowY": "auto",                 # rolagem vertical
                    },
                    style_cell={
                        "padding": "10px",
                        "textAlign": "left",
                        "border": "0",
                        "whiteSpace": "normal",
                        "height": "auto",
                    },
                    style_header={"fontWeight": "700", "border": "0"},
                    style_cell_conditional=[
                        {"if": {"column_id": "nome_do_veiculo"},
                         "minWidth": "260px", "width": "320px", "maxWidth": "520px"},
                        {"if": {"column_id": "cidade"},
                         "minWidth": "120px", "width": "140px", "maxWidth": "200px"},
                        {"if": {"column_id": "status"},
                         "minWidth": "120px", "width": "140px", "maxWidth": "200px"},
                        {"if": {"column_id": "motivo"},
                         "minWidth": "240px", "width": "360px", "maxWidth": "560px"},
                        {"if": {"column_id": "media_trimestral"}, "textAlign": "right"},
                    ],
                ),
            ]),
        ]),
    ]),
])

# ========= CALLBACKS =========
def _filtrar(base: pd.DataFrame, cidade, status, categoria, termo) -> pd.DataFrame:
    dff = base.copy()
    if cidade:   dff = dff[dff["cidade"].isin(cidade)]
    if status:   dff = dff[dff["status"].isin(status)]
    if categoria:dff = dff[dff["categoria"].isin(categoria)]
    if termo and str(termo).strip():
        alvo = "nome_fantasia" if "nome_fantasia" in dff.columns else (
            "nome_do_veiculo" if "nome_do_veiculo" in dff.columns else dff.columns[0]
        )
        dff = dff[dff[alvo].astype(str).str.contains(str(termo), case=False, na=False)]
    return dff

@app.callback(Output("root", "className"), Input("theme-toggle", "value"))
def set_theme(theme): return "light" if theme == "light" else "dark"

# Atualiza KPI/Gráficos/Tabela
@app.callback(
    Output("kpi_total", "children"),
    Output("kpi_aprov", "children"),
    Output("kpi_reprov", "children"),
    Output("kpi_cidades", "children"),
    Output("g_status", "figure"),
    Output("g_top_cidades", "figure"),
    Output("g_meses", "figure"),
    Output("g_top_sites", "figure"),
    Output("tbl", "data"),
    Output("tbl", "columns"),
    Input("f_cidade", "value"),
    Input("f_status", "value"),
    Input("f_categoria", "value"),
    Input("f_busca", "value"),
    Input("sort-order", "value"),
    Input("btn-reload", "n_clicks"),
    State("theme-toggle", "value"),
)
def atualizar(f_cidade, f_status, f_categoria, f_busca, order, n_reload, theme):
    base = load_data() if (n_reload and n_reload > 0) else DF_BASE
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_busca)
    ascending = (order == "asc")

    total = int(len(dff))
    aprov = int((dff["status"] == "APROVADO").sum()) if "status" in dff else 0
    reprov = int((dff["status"] == "REPROVADO").sum()) if "status" in dff else 0
    cidades_qtd = int(dff["cidade"].nunique()) if "cidade" in dff else 0

    # Status
    if "status" in dff and not dff.empty:
        g1 = dff["status"].astype(str).str.upper().value_counts().reset_index()
        g1.columns = ["status", "qtd"]
        g1 = g1.sort_values("qtd", ascending=ascending)
        fig_status = px.bar(
            g1, x="status", y="qtd", text="qtd", title="Distribuição por Status",
            color="status",
            color_discrete_map={
                "APROVADO": "#22C55E",
                "REPROVADO": "#EF4444",
                "APROVADO PARCIAL": "#F59E0B",
                "PENDENTE": "#A78BFA",
                "INSTA": "#06B6D4",
            },
        )
        fig_status.update_traces(textposition="outside")
        fig_status.update_layout(
            showlegend=True,
            xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()),
        )
    else:
        fig_status = px.bar(title="Distribuição por Status")
    style_fig(fig_status, theme)

    # Top cidades
    if "cidade" in dff and not dff.empty:
        base_cid = dff["cidade"].value_counts().reset_index()
        base_cid.columns = ["cidade", "qtd"]
        base_cid = base_cid.sort_values("qtd", ascending=False).head(10)
        base_cid = base_cid.sort_values("qtd", ascending=ascending)
        seq = get_sequence(theme, len(base_cid))
        fig_cidades = px.bar(
            base_cid, x="cidade", y="qtd", text="qtd", title="Top 10 Cidades",
            color="cidade", color_discrete_sequence=seq,
        )
        fig_cidades.update_traces(textposition="outside")
        fig_cidades.update_layout(
            showlegend=False,
            xaxis=dict(categoryorder="array", categoryarray=base_cid["cidade"].tolist()),
        )
    else:
        fig_cidades = px.bar(title="Top 10 Cidades")
    style_fig(fig_cidades, theme)

    # Visualizações por mês
    vj = float(dff["visualizacoes_junho"].sum()) if "visualizacoes_junho" in dff else 0.0
    vl = float(dff["visualizacoes_julho"].sum()) if "visualizacoes_julho" in dff else 0.0
    va = float(dff["visualizacoes_agosto"].sum()) if "visualizacoes_agosto" in dff else 0.0
    g3 = pd.DataFrame({"Mês": ["Junho", "Julho", "Agosto"], "Visualizações": [vj, vl, va]}).sort_values(
        "Visualizações", ascending=ascending
    )
    seq3 = get_sequence(theme, len(g3))
    fig_meses = px.bar(
        g3, x="Mês", y="Visualizações", text="Visualizações",
        title="Total de Visualizações por Mês",
        color="Mês", color_discrete_sequence=seq3,
    )
    fig_meses.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig_meses.update_layout(
        showlegend=False,
        xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()),
    )
    style_fig(fig_meses, theme)

    # Top sites
    if {"nome_fantasia", "total_visualizacoes"}.issubset(dff.columns) and not dff.empty:
        g4 = dff.nlargest(10, "total_visualizacoes")[["nome_fantasia", "total_visualizacoes"]]
        g4 = g4.sort_values("total_visualizacoes", ascending=ascending)
        seq4 = get_sequence(theme, len(g4))
        fig_sites = px.bar(
            g4, x="total_visualizacoes", y="nome_fantasia", orientation="h",
            text="total_visualizacoes",
            title="Top 10 Sites (Total de Visualizações)",
            color="nome_fantasia", color_discrete_sequence=seq4,
        )
        fig_sites.update_traces(texttemplate="%{text:.0f}")
        fig_sites.update_layout(
            showlegend=False,
            yaxis=dict(categoryorder="array", categoryarray=g4["nome_fantasia"].tolist()),
        )
    else:
        fig_sites = px.bar(title="Top 10 Sites (Total de Visualizações)")
    style_fig(fig_sites, theme)

    # Tabela (Média Trimestral, sem categoria/jun/jul/ago) — lista contínua
    cols_order = [
        "nome_do_veiculo", "cidade", "status", "motivo", "media_trimestral"
    ]
    friendly = {
        "nome_do_veiculo": "Nome do Veículo",
        "cidade": "Cidade", "status": "Status", "motivo": "Motivo",
        "media_trimestral": "Média Trimestral",
    }
    present = [c for c in cols_order if c in dff.columns]
    fmt_int = Format(group=Group.yes, groups=3, group_delimiter=".", decimal_delimiter=",",
                     precision=0, scheme=Scheme.fixed)
    columns = []
    for c in present:
        col_def = {"name": friendly.get(c, c), "id": c}
        if c in ["media_trimestral"]:
            col_def.update({"type": "numeric", "format": fmt_int})
        columns.append(col_def)
    data = dff[present].to_dict("records")

    return (
        f"{total}", f"{aprov}", f"{reprov}", f"{cidades_qtd}",
        fig_status, fig_cidades, fig_meses, fig_sites,
        data, columns,
    )

# Atualiza opções dos filtros ao clicar em "Atualizar dados"
@app.callback(
    Output("f_cidade", "options"),
    Output("f_status", "options"),
    Output("f_categoria", "options"),
    Input("btn-reload", "n_clicks"),
    prevent_initial_call=True
)
def refresh_filter_options(n):
    d = load_data()
    cidades = [{"label": c, "value": c} for c in sorted(d["cidade"].dropna().unique())] if "cidade" in d else []
    status  = [{"label": s, "value": s} for s in sorted(d["status"].dropna().unique())] if "status" in d else []
    cats    = [{"label": c, "value": c} for c in sorted(d["categoria"].dropna().unique())] if "categoria" in d else []
    return cidades, status, cats

# ========= EXPORTS =========
# Excel (preferencial) com fallback para CSV se faltar engine
@app.callback(
    Output("download_excel", "data"),
    Input("btn-export-excel", "n_clicks"),
    State("f_cidade", "value"),
    State("f_status", "value"),
    State("f_categoria", "value"),
    State("f_busca", "value"),
    prevent_initial_call=True
)
def exportar_excel(n, f_cidade, f_status, f_categoria, f_busca):
    df = _filtered_df_for_export(f_cidade, f_status, f_categoria, f_busca)
    try:
        return dcc.send_data_frame(
            df.to_excel,
            "metricas_de_veiculos.xlsx",
            sheet_name="Dados",
            index=False
        )
    except Exception as e:
        print("[export_excel] Falhou to_excel, fallback para CSV:", e)
        return dcc.send_data_frame(
            df.to_csv,
            "metricas_de_veiculos.csv",
            index=False
        )

def _filtered_df_for_export(f_cidade, f_status, f_categoria, f_busca) -> pd.DataFrame:
    base = load_data()
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_busca)
    cols_export = [
        "nome_do_veiculo", "cidade", "status", "motivo", "media_trimestral"
    ]
    return dff[[c for c in cols_export if c in dff.columns]].copy()

# ---- PDF (gráficos + tabela ajustada à página)
@app.callback(
    Output("download_pdf", "data"),
    Input("btn-export-pdf", "n_clicks"),
    State("f_cidade", "value"),
    State("f_status", "value"),
    State("f_categoria", "value"),
    State("f_busca", "value"),
    State("sort-order", "value"),
    State("theme-toggle", "value"),
    prevent_initial_call=True
)
def exportar_pdf(n, f_cidade, f_status, f_categoria, f_busca, order, theme):
    # dados filtrados
    base = load_data()
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_busca)
    ascending = (order == "asc")

    # Gera figuras (para PDF, forçamos tema CLARO para imprimir melhor)
    pdf_theme = "light"

    # --- Fig Status
    if "status" in dff and not dff.empty:
        g1 = dff["status"].astype(str).str.upper().value_counts().reset_index()
        g1.columns = ["status", "qtd"]
        g1 = g1.sort_values("qtd", ascending=ascending)
        fig_status = px.bar(
            g1, x="status", y="qtd", text="qtd", title="Distribuição por Status",
            color="status",
            color_discrete_map={
                "APROVADO": "#22C55E",
                "REPROVADO": "#EF4444",
                "APROVADO PARCIAL": "#F59E0B",
                "PENDENTE": "#A78BFA",
                "INSTA": "#06B6D4",
            },
        )
        fig_status.update_traces(textposition="outside")
        fig_status.update_layout(showlegend=True,
                                 xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()))
    else:
        fig_status = px.bar(title="Distribuição por Status")
    style_fig(fig_status, pdf_theme)
    fig_status.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    # --- Fig Top Cidades
    if "cidade" in dff and not dff.empty:
        base_cid = dff["cidade"].value_counts().reset_index()
        base_cid.columns = ["cidade", "qtd"]
        base_cid = base_cid.sort_values("qtd", ascending=False).head(10)
        base_cid = base_cid.sort_values("qtd", ascending=ascending)
        seq = get_sequence(pdf_theme, len(base_cid))
        fig_cidades = px.bar(
            base_cid, x="cidade", y="qtd", text="qtd", title="Top 10 Cidades",
            color="cidade", color_discrete_sequence=seq,
        )
        fig_cidades.update_traces(textposition="outside")
        fig_cidades.update_layout(showlegend=False,
                                  xaxis=dict(categoryorder="array", categoryarray=base_cid["cidade"].tolist()))
    else:
        fig_cidades = px.bar(title="Top 10 Cidades")
    style_fig(fig_cidades, pdf_theme)
    fig_cidades.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    # --- Fig Meses
    vj = float(dff["visualizacoes_junho"].sum()) if "visualizacoes_junho" in dff else 0.0
    vl = float(dff["visualizacoes_julho"].sum()) if "visualizacoes_julho" in dff else 0.0
    va = float(dff["visualizacoes_agosto"].sum()) if "visualizacoes_agosto" in dff else 0.0
    g3 = pd.DataFrame({"Mês": ["Junho", "Julho", "Agosto"], "Visualizações": [vj, vl, va]}).sort_values(
        "Visualizações", ascending=ascending
    )
    seq3 = get_sequence(pdf_theme, len(g3))
    fig_meses = px.bar(
        g3, x="Mês", y="Visualizações", text="Visualizações",
        title="Total de Visualizações por Mês",
        color="Mês", color_discrete_sequence=seq3,
    )
    fig_meses.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig_meses.update_layout(showlegend=False,
                            xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()))
    style_fig(fig_meses, pdf_theme)
    fig_meses.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    # --- Fig Top Sites
    if {"nome_fantasia", "total_visualizacoes"}.issubset(dff.columns) and not dff.empty:
        g4 = dff.nlargest(10, "total_visualizacoes")[["nome_fantasia", "total_visualizacoes"]]
        g4 = g4.sort_values("total_visualizacoes", ascending=ascending)
        seq4 = get_sequence(pdf_theme, len(g4))
        fig_sites = px.bar(
            g4, x="total_visualizacoes", y="nome_fantasia", orientation="h",
            text="total_visualizacoes",
            title="Top 10 Sites (Total de Visualizações)",
            color="nome_fantasia", color_discrete_sequence=seq4,
        )
        fig_sites.update_traces(texttemplate="%{text:.0f}")
        fig_sites.update_layout(showlegend=False,
                                yaxis=dict(categoryorder="array", categoryarray=g4["nome_fantasia"].tolist()))
    else:
        fig_sites = px.bar(title="Top 10 Sites (Total de Visualizações)")
    style_fig(fig_sites, pdf_theme)
    fig_sites.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    figs = [fig_status, fig_cidades, fig_meses, fig_sites]

    def _to_pdf(bytes_io):
        # libs do ReportLab
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.units import mm

        page_size = landscape(A4)
        left_margin = right_margin = top_margin = bottom_margin = 12 * mm
        avail_w = page_size[0] - left_margin - right_margin

        doc = SimpleDocTemplate(
            bytes_io,
            pagesize=page_size,
            leftMargin=left_margin, rightMargin=right_margin,
            topMargin=top_margin, bottomMargin=bottom_margin
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle("title", parent=styles["Heading2"],
                                     alignment=0, fontSize=14, leading=16,
                                     textColor=colors.HexColor("#111827"))

        header_style = ParagraphStyle("header", parent=styles["Normal"], fontSize=9, textColor=colors.white)
        cell_text = ParagraphStyle("cell", parent=styles["Normal"], fontSize=8, leading=10)
        cell_wrap = ParagraphStyle("cell_wrap", parent=cell_text, wordWrap="CJK")

        story = []
        story.append(Paragraph("Métricas de Veículos — Relatório", title_style))
        story.append(Spacer(1, 6))

        # ====== GRÁFICOS (2 por linha) ======
        def fig_to_rlimage(fig, width_pt):
            """Converte figure Plotly em RLImage com largura específica."""
            try:
                img_bytes = pio.to_image(fig, format="png", scale=2)  # requer kaleido
                # Ajuste de altura mantendo proporção 16:9 aproximada
                height_pt = width_pt * 9.0 / 16.0
                return RLImage(BytesIO(img_bytes), width=width_pt, height=height_pt)
            except Exception as e:
                print("[export_pdf] Falha ao renderizar gráfico com kaleido:", e)
                return None

        col_w = (avail_w - 6*mm) / 2.0  # 6mm de "gutter" entre colunas
        row_imgs = []
        for i, fig in enumerate(figs):
            rlimg = fig_to_rlimage(fig, col_w)
            if rlimg:
                row_imgs.append(rlimg)
            else:
                row_imgs.append(Paragraph("**Gráfico indisponível (kaleido ausente)**", cell_text))

            # a cada 2 imagens, fecha a linha
            if (i % 2 == 1) or (i == len(figs)-1):
                t = Table([[row_imgs[0]] + ([row_imgs[1]] if len(row_imgs) > 1 else [])],
                          colWidths=[col_w, col_w] if len(row_imgs) > 1 else [col_w])
                t.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP"),
                                       ("LEFTPADDING", (0,0), (-1,-1), 0),
                                       ("RIGHTPADDING", (0,0), (-1,-1), 0),
                                       ("TOPPADDING", (0,0), (-1,-1), 0),
                                       ("BOTTOMPADDING", (0,0), (-1,-1), 0)]))
                story.append(t)
                story.append(Spacer(1, 8))
                row_imgs = []

        # ====== TABELA ======
        labels = {
            "nome_do_veiculo": "Nome do Veículo",
            "cidade": "Cidade",
            "status": "Status",
            "motivo": "Motivo",
            "media_trimestral": "Média Trimestral",
        }
        col_keys = ["nome_do_veiculo","cidade","status","motivo","media_trimestral"]
        col_keys = [c for c in col_keys if c in dff.columns]
        headers = [labels[k] for k in col_keys]

        # cabeçalho
        data = [[Paragraph(h, header_style) for h in headers]]

        # função para números
        def fmt_int(x):
            try:
                return str(f"{int(round(float(x))):,}").replace(",", ".")
            except:
                return str(x)

        for _, row in dff[col_keys].iterrows():
            line = []
            for k in col_keys:
                val = row[k]
                if k in ["media_trimestral"]:
                    line.append(Paragraph(fmt_int(val), cell_text))
                elif k in ["nome_do_veiculo","motivo"]:
                    line.append(Paragraph(str(val), cell_wrap))
                else:
                    line.append(Paragraph(str(val), cell_text))
            data.append(line)

        # Ajuste proporcional de larguras para caber
        weights = {
            "nome_do_veiculo": 3.8,
            "cidade": 1.3,
            "status": 1.2,
            "motivo": 3.8,
            "media_trimestral": 1.6,
        }
        wlist = [weights.get(k, 1.0) for k in col_keys]
        total_w = sum(wlist)
        col_widths = [ (w/total_w) * avail_w for w in wlist ]

        tbl = Table(data, colWidths=col_widths, repeatRows=1)
        # Permite quebra por linha para caber em várias páginas
        tbl.splitByRow = 1

        styles_tbl = [
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#111827")),
            ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
            ("ALIGN",      (0,0), (-1,0), "CENTER"),
            ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
            ("GRID",       (0,0), (-1,-1), 0.25, colors.HexColor("#D1D5DB")),
            ("VALIGN",     (0,0), (-1,-1), "TOP"),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.HexColor("#F8FAFC")]),
            ("LEFTPADDING", (0,0), (-1,-1), 4),
            ("RIGHTPADDING", (0,0), (-1,-1), 4),
            ("TOPPADDING", (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ]
        # alinhar números à direita
        idx_media = [i for i, k in enumerate(col_keys) if k == "media_trimestral"]
        for idx in idx_media:
            styles_tbl.append(("ALIGN", (idx,1), (idx,-1), "RIGHT"))

        tbl.setStyle(TableStyle(styles_tbl))
        story.append(Spacer(1, 4))
        story.append(tbl)
        doc.build(story)

    return dcc.send_bytes(_to_pdf, "metricas_de_veiculos.pdf")

# RUN
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    try:
        app.run(debug=True, host="0.0.0.0", port=port)
    except AttributeError:
        app.run_server(debug=True, host="0.0.0.0", port=port)
