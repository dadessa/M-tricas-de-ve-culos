# -*- coding: utf-8 -*-
"""
Observatório – Métricas de Veículos  (Sheets/Excel)
- Tema claro/escuro
- Filtros: cidade, status, categoria, motivo + busca por nome e ordenação
- KPIs, gráficos e tabela (sem paginação)
- REPROVADO sempre vermelho no gráfico de Status
- Colunas de visualizações por mês detectadas de forma robusta
"""

import os
import io
import unicodedata
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output, State
from dash.dcc import send_data_frame

# =========================================================
# Config: fonte dos dados
# =========================================================
EXCEL_PATH = "Recadastramento (respostas).xlsx"
SHEETS_CSV_URL = os.environ.get("SHEETS_CSV_URL", "").strip()  # opcional


# =========================================================
# Helpers de normalização/limpeza
# =========================================================
def _normalize(colname: str) -> str:
    s = str(colname).strip().replace("\n", " ")
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    s = s.lower().replace("  ", " ").replace(" ", "_")
    return s


def clean_numeric(series: pd.Series) -> pd.Series:
    """Remove texto, trata vírgula/ponto e converte para float."""
    s = series.astype(str)
    s = s.str.replace(r"[^0-9,.\-]+", "", regex=True)  # deixa apenas dígitos, , . e sinal
    s = s.str.replace(",", ".", regex=False)          # vírgula decimal -> ponto
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _pick_col(cols: list[str], aliases: list[str], contains_all: list[str] | None = None) -> str | None:
    # tenta match exato primeiro
    for a in aliases:
        if a in cols:
            return a
    # depois por "contains"
    if contains_all:
        for c in cols:
            if all(k in c for k in contains_all):
                return c
    return None


def _find_month_source(cols: list[str], mes_slug: str) -> str | None:
    """
    Localiza a melhor coluna para o mês informado (junho/julho/agosto).
    Aceita várias grafias e padrões.
    """
    aliases = [
        f"visualizacoes_{mes_slug}",
        f"vizualizacoes_{mes_slug}",
        f"total_de_visualizacoes_{mes_slug}",
        f"total_de_vizualizacoes_{mes_slug}",
        f"total_visualizacoes_{mes_slug}",
        f"total_vizualizacoes_{mes_slug}",
    ]
    hit = _pick_col(cols, aliases)
    if hit:
        return hit
    # padrões "mes + visual" ou "mes + vizual"
    hit = _pick_col(cols, [], contains_all=[mes_slug, "visual"])
    if hit:
        return hit
    hit = _pick_col(cols, [], contains_all=[mes_slug, "vizual"])
    return hit


def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # normaliza cabeçalhos
    original_cols = list(df.columns)
    norm_map = {c: _normalize(c) for c in original_cols}
    df.columns = [norm_map[c] for c in original_cols]
    cols = list(df.columns)

    # renomeios comuns
    rename_map = {
        "nome_do_veiculo.": "nome_fantasia",
        "nome_empresarial_da_empresa_responsavel.": "razao_social",
        "endereco_no_site": "endereco_site",
        "url_ativa_do_veiculo.": "url",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # campos básicos
    for c in ["cidade", "categoria", "status"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        else:
            df[c] = "Não informado"
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper()

    # nome do veículo exibido
    if "nome_fantasia" in df.columns:
        df["nome_do_veiculo"] = df["nome_fantasia"].astype(str)
    else:
        possiveis_nome = [c for c in cols if "nome" in c and "veiculo" in c]
        df["nome_do_veiculo"] = df[possiveis_nome[0]].astype(str) if possiveis_nome else ""

    if "url" not in df.columns:
        df["url"] = ""

    # MOTIVO (evita string "nan")
    motivo_candidates = [c for c in cols if any(k in c for k in ["motivo", "indefer", "observa", "justific", "coment"])]
    if motivo_candidates:
        col_motivo = motivo_candidates[0]
        df["motivo"] = df[col_motivo].astype(str).replace({"nan": "", "NaN": ""})
    else:
        df["motivo"] = ""

    # localizar colunas de visualizações por mês (bem tolerante)
    cols = list(df.columns)
    src_junho = _find_month_source(cols, "junho")
    src_julho = _find_month_source(cols, "julho")
    src_agosto = _find_month_source(cols, "agosto")

    # cria/atualiza colunas padronizadas
    df["visualizacoes_junho"] = clean_numeric(df[src_junho]) if src_junho else 0.0
    df["visualizacoes_julho"] = clean_numeric(df[src_julho]) if src_julho else 0.0
    df["visualizacoes_agosto"] = clean_numeric(df[src_agosto]) if src_agosto else 0.0

    # total
    df["total_visualizacoes"] = (
        df["visualizacoes_junho"] + df["visualizacoes_julho"] + df["visualizacoes_agosto"]
    )

    return df


# =========================================================
# Carregamento dos dados (Sheets CSV OU Excel local)
# =========================================================
def load_data() -> pd.DataFrame:
    if SHEETS_CSV_URL:
        # Leitura de CSV do Google Sheets (usar link "Export as CSV")
        base = pd.read_csv(SHEETS_CSV_URL, dtype=str, keep_default_na=False)
    else:
        base = pd.read_excel(EXCEL_PATH, dtype=str)
    return _prepare_df(base)


# Carrega base inicial
DF_BASE = load_data()


# =========================================================
# Tema e cores
# =========================================================
THEME_COLORS = {
    "light": {
        "font": "#0F172A",
        "muted": "#64748B",
        "grid": "#E9EDF5",
        "paper": "rgba(0,0,0,0)",
        "plot": "rgba(0,0,0,0)",
        "colorway": ["#3B82F6", "#22C55E", "#F59E0B", "#EF4444", "#06B6D4", "#A78BFA"],
        "template": "plotly_white",
    },
    "dark": {
        "font": "#E6ECFF",
        "muted": "#93A3BE",
        "grid": "#22304A",
        "paper": "rgba(0,0,0,0)",
        "plot": "rgba(0,0,0,0)",
        "colorway": ["#60A5FA", "#34D399", "#FBBF24", "#F87171", "#22D3EE", "#CABFFD"],
        "template": "plotly_dark",
    },
}

EXTENDED_SEQ = {
    "light": [
        "#3B82F6", "#22C55E", "#F59E0B", "#EF4444", "#06B6D4", "#A78BFA",
        "#10B981", "#F43F5E", "#8B5CF6", "#14B8A6", "#EAB308", "#0EA5E9",
    ],
    "dark": [
        "#60A5FA", "#34D399", "#FBBF24", "#F87171", "#22D3EE", "#CABFFD",
        "#4ADE80", "#FB7185", "#A78BFA", "#2DD4BF", "#FACC15", "#38BDF8",
    ],
}


def get_sequence(theme: str, n: int):
    seq = EXTENDED_SEQ.get(theme, EXTENDED_SEQ["light"])
    if n <= len(seq):
        return seq[:n]
    times = (n // len(seq)) + 1
    return (seq * times)[:n]


def style_fig(fig, theme="light"):
    c = THEME_COLORS[theme]
    fig.update_layout(
        template=c["template"],
        colorway=c["colorway"],
        paper_bgcolor=c["paper"],
        plot_bgcolor=c["plot"],
        font=dict(color=c["font"], size=13),
        title=dict(font=dict(color=c["font"], size=16)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=c["font"])),
        margin=dict(l=12, r=12, t=48, b=12),
        uniformtext_minsize=12,
        uniformtext_mode="hide",
        hoverlabel=dict(
            bgcolor="rgba(15,23,42,0.95)" if theme == "dark" else "#ffffff",
            font_color=c["font"],
            bordercolor="rgba(0,0,0,0)",
        ),
    )
    fig.update_xaxes(
        gridcolor=c["grid"],
        zerolinecolor=c["grid"],
        tickfont=dict(color=c["font"]),
        title_font=dict(color=c["font"]),
    )
    fig.update_yaxes(
        gridcolor=c["grid"],
        zerolinecolor=c["grid"],
        tickfont=dict(color=c["font"]),
        title_font=dict(color=c["font"]),
    )
    fig.update_traces(
        selector=dict(type="bar"),
        textfont_color=c["font"],
        marker_line_width=0,
        opacity=0.95 if theme == "dark" else 1.0,
    )
    return fig


# =========================================================
# App
# =========================================================
app = Dash(__name__)
server = app.server


def kpi_card(kpi_id: str, label: str):
    return html.Div(className="card kpi", children=[
        html.P(label, className="kpi-title"),
        html.H2(id=kpi_id, className="kpi-value"),
    ])


app.layout = html.Div(className="light", id="root", children=[
    dcc.Store(id="store-base", data=DF_BASE.to_json(orient="split")),

    html.Div(className="container", children=[
        # Navbar
        html.Div(className="navbar", children=[
            html.Div(className="brand", children=[
                html.Div("📊", style={"fontSize": "20px"}),
                html.H1("Métricas de Veículos"),
                html.Span("v2.3 (Sheets/Excel)", className="badge"),
            ]),
            html.Div(className="actions", children=[
                dcc.RadioItems(
                    id="theme-toggle",
                    options=[{"label": "Claro", "value": "light"},
                             {"label": "Escuro", "value": "dark"}],
                    value="light",
                    inline=True,
                    inputStyle={"marginRight": "6px", "marginLeft": "10px"},
                    style={"marginRight": "8px"},
                ),
                html.Button("Atualizar dados", id="btn-reload", n_clicks=0, className="btn ghost"),
                html.Button("Exportar Excel", id="btn-export", n_clicks=0, className="btn"),
                dcc.Download(id="download"),
            ]),
        ]),

        # Filtros
        html.Div(className="panel", children=[
            html.Div(className="filters", children=[
                html.Div(children=[
                    html.Div("Cidade", className="label"),
                    dcc.Dropdown(
                        id="f_cidade",
                        options=[{"label": c, "value": c} for c in sorted(DF_BASE["cidade"].dropna().unique())],
                        multi=True,
                        placeholder="Selecione cidades…",
                    ),
                ]),
                html.Div(children=[
                    html.Div("Status", className="label"),
                    dcc.Dropdown(
                        id="f_status",
                        options=[{"label": s, "value": s} for s in sorted(DF_BASE["status"].dropna().unique())],
                        multi=True,
                        placeholder="Selecione status…",
                    ),
                ]),
                html.Div(children=[
                    html.Div("Categoria", className="label"),
                    dcc.Dropdown(
                        id="f_categoria",
                        options=[{"label": c, "value": c} for c in sorted(DF_BASE["categoria"].dropna().unique())],
                        multi=True,
                        placeholder="Selecione categorias…",
                    ),
                ]),
                html.Div(children=[
                    html.Div("Motivo", className="label"),
                    dcc.Dropdown(
                        id="f_motivo",
                        options=[{"label": m if m else "(vazio)", "value": m}
                                 for m in sorted(DF_BASE["motivo"].fillna("").unique())],
                        multi=True,
                        placeholder="Selecione motivos…",
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
                        value="desc",
                        inline=True,
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
        html.Div(className="card", children=[dcc.Graph(id="g_motivo", config={"displayModeBar": False})]),

        # Tabela (lista única, sem paginação)
        html.Div(className="panel", children=[
            html.Div("Dados detalhados", className="label"),
            html.Div(className="card", children=[
                dash_table.DataTable(
                    id="tbl",
                    sort_action="native",
                    filter_action="native",
                    page_action="none",                     # sem paginação
                    style_table={
                        "overflowX": "auto", "maxWidth": "100%",
                        "height": "600px", "overflowY": "auto"  # scroll vertical
                    },
                    style_cell={
                        "padding": "10px",
                        "textAlign": "left",
                        "border": "0",
                        "whiteSpace": "normal",
                        "height": "auto",
                        "minWidth": "100px",
                    },
                    style_header={"fontWeight": "700", "border": "0"},
                    style_cell_conditional=[
                        {"if": {"column_id": "nome_do_veiculo"}, "minWidth": "260px", "maxWidth": "420px"},
                        {"if": {"column_id": "cidade"}, "width": "160px", "maxWidth": "200px"},
                        {"if": {"column_id": "status"}, "width": "140px", "maxWidth": "160px"},
                        {"if": {"column_id": "motivo"}, "minWidth": "240px", "maxWidth": "520px"},
                        {"if": {"column_id": "categoria"}, "width": "180px", "maxWidth": "220px"},
                        {"if": {"column_id": "visualizacoes_junho"}, "textAlign": "right", "width": "160px"},
                        {"if": {"column_id": "visualizacoes_julho"}, "textAlign": "right", "width": "160px"},
                        {"if": {"column_id": "visualizacoes_agosto"}, "textAlign": "right", "width": "160px"},
                    ],
                    style_data_conditional=[
                        {"if": {"filter_query": "{status} = REPROVADO", "column_id": "status"},
                         "color": "#EF4444", "fontWeight": "700"},
                        {"if": {"state": "active"}, "backgroundColor": "rgba(180,200,255,0.10)"},
                    ],
                ),
            ]),
        ]),
    ]),
])


# =========================================================
# Callbacks
# =========================================================

# Atualiza o Store ao clicar em "Atualizar dados"
@app.callback(
    Output("store-base", "data"),
    Input("btn-reload", "n_clicks"),
    prevent_initial_call=True,
)
def do_reload(n_clicks):
    df = load_data()  # recarrega da fonte configurada (Sheets CSV ou Excel)
    return df.to_json(orient="split")


def _filtrar(base: pd.DataFrame, cidade, status, categoria, motivo, termo) -> pd.DataFrame:
    dff = base.copy()
    if cidade:
        dff = dff[dff["cidade"].isin(cidade)]
    if status:
        dff = dff[dff["status"].isin(status)]
    if categoria:
        dff = dff[dff["categoria"].isin(categoria)]
    if motivo:
        # valores vazios representados por ""
        dff = dff[dff["motivo"].isin(motivo)]
    if termo and str(termo).strip():
        alvo = "nome_do_veiculo" if "nome_do_veiculo" in dff.columns else dff.columns[0]
        dff = dff[dff[alvo].astype(str).str.contains(str(termo), case=False, na=False)]
    return dff


@app.callback(Output("root", "className"), Input("theme-toggle", "value"))
def set_theme(theme):
    return "light" if theme == "light" else "dark"


@app.callback(
    Output("kpi_total", "children"),
    Output("kpi_aprov", "children"),
    Output("kpi_reprov", "children"),
    Output("kpi_cidades", "children"),
    Output("g_status", "figure"),
    Output("g_top_cidades", "figure"),
    Output("g_meses", "figure"),
    Output("g_top_sites", "figure"),
    Output("g_motivo", "figure"),
    Output("tbl", "data"),
    Output("tbl", "columns"),
    Input("store-base", "data"),
    Input("f_cidade", "value"),
    Input("f_status", "value"),
    Input("f_categoria", "value"),
    Input("f_motivo", "value"),
    Input("f_busca", "value"),
    Input("sort-order", "value"),
    State("theme-toggle", "value"),
)
def atualizar(store_json, f_cidade, f_status, f_categoria, f_motivo, f_busca, order, theme):
    base = pd.read_json(store_json, orient="split") if store_json else DF_BASE
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_motivo, f_busca)

    ascending = (order == "asc")

    # KPIs
    total = int(len(dff))
    aprov = int((dff["status"] == "APROVADO").sum()) if "status" in dff else 0
    reprov = int((dff["status"] == "REPROVADO").sum()) if "status" in dff else 0
    cidades_qtd = int(dff["cidade"].nunique()) if "cidade" in dff else 0

    # Gráfico: Status (REPROVADO vermelho)
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
            xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()),
            showlegend=True,
        )
    else:
        fig_status = px.bar(title="Distribuição por Status")
    style_fig(fig_status, theme)

    # Gráfico: Top cidades
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

    # Gráfico: Visualizações por mês (usa colunas detectadas e tratadas)
    vj = float(dff["visualizacoes_junho"].sum()) if "visualizacoes_junho" in dff else 0.0
    vl = float(dff["visualizacoes_julho"].sum()) if "visualizacoes_julho" in dff else 0.0
    va = float(dff["visualizacoes_agosto"].sum()) if "visualizacoes_agosto" in dff else 0.0
    g3 = pd.DataFrame({"Mês": ["Junho", "Julho", "Agosto"],
                       "Visualizações": [vj, vl, va]}).sort_values("Visualizações", ascending=ascending)
    seq3 = get_sequence(theme, len(g3))
    fig_meses = px.bar(
        g3, x="Mês", y="Visualizações", text="Visualizações",
        title="Total de Visualizações por Mês", color="Mês",
        color_discrete_sequence=seq3,
    )
    fig_meses.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig_meses.update_layout(
        showlegend=False,
        xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()),
    )
    style_fig(fig_meses, theme)

    # Gráfico: Top sites por total
    if {"nome_do_veiculo", "total_visualizacoes"}.issubset(dff.columns) and not dff.empty:
        g4 = dff.nlargest(10, "total_visualizacoes")[["nome_do_veiculo", "total_visualizacoes"]]
        g4 = g4.sort_values("total_visualizacoes", ascending=ascending)
        seq4 = get_sequence(theme, len(g4))
        fig_sites = px.bar(
            g4, x="total_visualizacoes", y="nome_do_veiculo", orientation="h",
            text="total_visualizacoes", title="Top 10 Sites (Total de Visualizações)",
            color="nome_do_veiculo", color_discrete_sequence=seq4,
        )
        fig_sites.update_traces(texttemplate="%{text:.0f}")
        fig_sites.update_layout(
            showlegend=False,
            yaxis=dict(categoryorder="array", categoryarray=g4["nome_do_veiculo"].tolist()),
        )
    else:
        fig_sites = px.bar(title="Top 10 Sites (Total de Visualizações)")
    style_fig(fig_sites, theme)

    # Gráfico: Motivo (frequência)
    if "motivo" in dff and not dff.empty:
        g5 = dff["motivo"].fillna("").replace({"": "(vazio)"}).value_counts().reset_index()
        g5.columns = ["motivo", "qtd"]
        g5 = g5.sort_values("qtd", ascending=ascending)
        seq5 = get_sequence(theme, len(g5))
        fig_motivo = px.bar(
            g5, x="motivo", y="qtd", text="qtd", title="Frequência por Motivo",
            color="motivo", color_discrete_sequence=seq5,
        )
        fig_motivo.update_traces(textposition="outside")
        fig_motivo.update_layout(showlegend=False,
                                 xaxis=dict(categoryorder="array", categoryarray=g5["motivo"].tolist()))
    else:
        fig_motivo = px.bar(title="Frequência por Motivo")
    style_fig(fig_motivo, theme)

    # Tabela (colunas fixas)
    cols_order = [
        "nome_do_veiculo", "cidade", "status", "motivo",
        "categoria", "visualizacoes_junho", "visualizacoes_julho", "visualizacoes_agosto",
    ]
    friendly = {
        "nome_do_veiculo": "Nome do Veículo",
        "cidade": "Cidade",
        "status": "Status",
        "motivo": "Motivo",
        "categoria": "Categoria",
        "visualizacoes_junho": "Visualizações Junho",
        "visualizacoes_julho": "Visualizações Julho",
        "visualizacoes_agosto": "Visualizações Agosto",
    }
    present = [c for c in cols_order if c in dff.columns]
    data = dff[present].to_dict("records")
    columns = [{"name": friendly[c], "id": c} for c in present]

    return (
        f"{total}", f"{aprov}", f"{reprov}", f"{cidades_qtd}",
        fig_status, fig_cidades, fig_meses, fig_sites, fig_motivo,
        data, columns,
    )


# Exportar Excel (fatia filtrada que está na tela)
@app.callback(
    Output("download", "data"),
    Input("btn-export", "n_clicks"),
    State("store-base", "data"),
    State("f_cidade", "value"),
    State("f_status", "value"),
    State("f_categoria", "value"),
    State("f_motivo", "value"),
    State("f_busca", "value"),
    prevent_initial_call=True,
)
def exportar_excel(n, store_json, f_cidade, f_status, f_categoria, f_motivo, f_busca):
    base = pd.read_json(store_json, orient="split") if store_json else DF_BASE
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_motivo, f_busca)
    cols_export = [
        "nome_do_veiculo", "cidade", "status", "motivo", "categoria",
        "visualizacoes_junho", "visualizacoes_julho", "visualizacoes_agosto", "total_visualizacoes",
    ]
    cols_export = [c for c in cols_export if c in dff.columns]
    return send_data_frame(dff[cols_export].to_excel, "dados_observatorio.xlsx", index=False)


# =========================================================
# Run
# =========================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    try:
        app.run(debug=True, host="0.0.0.0", port=port)
    except AttributeError:
        # compatibilidade com versões antigas do Dash
        app.run_server(debug=True, host="0.0.0.0", port=port)
