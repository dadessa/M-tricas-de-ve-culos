# -*- coding: utf-8 -*-
"""
Métricas de Veículos (v2.2 – Google Sheets com cache-buster)
- Lê direto do Google Sheets (aba gid=1225239898)
- Força atualização (cache-buster) para não usar CSV em cache
- Botão "Atualizar dados" e auto-reload a cada 5 min
- Filtros reativos (cidade/status/categoria/motivo) + busca por nome
- KPIs, gráficos e lista sem paginação
- Exporta Excel (formatado) e PDF (gráficos + lista)
"""

import os
import io
import time
import unicodedata
from datetime import datetime

import requests
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output, State, no_update
from dash.dcc import send_bytes

# ========= CONFIG DA PLANILHA =========
GOOGLE_SHEET_ID = "17TnGB6NpsziDec4fPH-d0TCQwk2LN0BAv6yjmIpyZnI"
SHEET_GID = 1225239898        # <- sua aba específica
EXCEL_FALLBACK = "Recadastramento (respostas).xlsx"  # opcional

def gsheet_csv_url(sheet_id: str, gid: int = 0) -> str:
    # cache-buster via timestamp para evitar cache de proxies
    ts = int(time.time())
    return (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export"
        f"?format=csv&gid={gid}&_cb={ts}"
    )

# ========= HELPERS =========
def _normalize(colname: str) -> str:
    s = str(colname).strip().replace("\n", "")
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    s = s.lower().replace(" ", "_")
    return s

def clean_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace(r"[^0-9,.-]", "", regex=True)
    s = s.str.replace(",", ".", regex=False)  # <- correto
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def _pick_motivo_column(cols: list[str]) -> str | None:
    preferidos = [
        "motivo","motivo_da_reprovacao","motivo_reprovacao",
        "motivo_do_status","motivo_reprovado","motivo_do_indeferimento",
    ]
    for c in preferidos:
        if c in cols: return c
    candidatos = [c for c in cols if "motivo" in c]
    if candidatos:
        reforcados = [c for c in candidatos if ("reprov" in c or "indefer" in c)]
        return reforcados[0] if reforcados else candidatos[0]
    outras = [c for c in cols if any(k in c for k in ["reprov","indefer","justific","observa","coment"])]
    return outras[0] if outras else None

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize(c) for c in df.columns]
    df = df.rename(columns={
        "nome_do_veiculo.":"nome_fantasia",
        "nome_empresarial_da_empresa_responsavel.":"razao_social",
        "endereco_no_site":"endereco_site",
        "url_ativa_do_veiculo.":"url",
        "total_de_visualizacoes_junho":"visualizacoes_junho",
        "total_de_vizualizacoes_junho":"visualizacoes_junho",
        "total_de_visualizacoes_julho":"visualizacoes_julho",
        "total_de_vizualizacoes_julho":"visualizacoes_julho",
        "total_de_visualizacoes_agosto":"visualizacoes_agosto",
        "total_de_vizualizacoes_agosto":"visualizacoes_agosto",
    })
    for c in ["visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]:
        if c not in df.columns:
            df[c] = 0
        df[c] = clean_numeric(df[c])
    df["total_visualizacoes"] = df[["visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]].sum(axis=1)

    for c in ["cidade","categoria","status"]:
        if c in df.columns: df[c] = df[c].astype(str).str.strip()
        else: df[c] = "Não informado"
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper()

    if "nome_fantasia" not in df.columns: df["nome_fantasia"] = ""
    df["nome_do_veiculo"] = df["nome_fantasia"].astype(str)
    if "url" not in df.columns: df["url"] = ""

    # Motivo
    motivo_col = _pick_motivo_column(list(df.columns))
    df["motivo"] = df[motivo_col].astype(str).fillna("").str.strip() if (motivo_col and motivo_col in df.columns) else ""
    return df

def load_data() -> pd.DataFrame:
    """
    Tenta ler do Google Sheets (CSV) com cache-buster e headers anti-cache.
    Se falhar, tenta fallback local (se existir).
    """
    url = gsheet_csv_url(GOOGLE_SHEET_ID, SHEET_GID)
    try:
        resp = requests.get(
            url,
            headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
            timeout=20,
        )
        resp.raise_for_status()
        # Lê CSV a partir do conteúdo baixado (evita problemas de encoding)
        csv_buf = io.StringIO(resp.content.decode("utf-8", errors="ignore"))
        base = pd.read_csv(csv_buf)
        return _prepare_df(base)
    except Exception as e_net:
        # fallback local
        try:
            base = pd.read_excel(EXCEL_FALLBACK)
            return _prepare_df(base)
        except Exception as e_local:
            raise RuntimeError(f"Falha ao ler do Sheets ({e_net}) e do Excel local ({e_local}).")

# ========= TEMA / CORES =========
THEME_COLORS = {
    "light": {"font":"#0F172A","muted":"#64748B","grid":"#E9EDF5",
              "paper":"rgba(0,0,0,0)","plot":"rgba(0,0,0,0)",
              "colorway":["#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA"],
              "template":"plotly_white"},
    "dark":  {"font":"#E6ECFF","muted":"#93A3BE","grid":"#22304A",
              "paper":"rgba(0,0,0,0)","plot":"rgba(0,0,0,0)",
              "colorway":["#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD"],
              "template":"plotly_dark"},
}
EXTENDED_SEQ = {
    "light":[ "#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA",
              "#10B981","#F43F5E","#8B5CF6","#14B8A6","#EAB308","#0EA5E9"],
    "dark":[  "#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD",
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
            bgcolor="rgba(15,23,42,0.95)" if theme=="dark" else "#ffffff",
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

# ========= BASE INICIAL =========
try:
    DF_BASE = load_data()
except Exception:
    DF_BASE = pd.DataFrame(columns=[
        "nome_do_veiculo","cidade","status","motivo","categoria",
        "visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto","total_visualizacoes"
    ])

# ========= APP =========
app = Dash(__name__)
server = app.server

def kpi_card(kpi_id: str, label: str):
    return html.Div(className="card kpi", children=[
        html.P(label, className="kpi-title"),
        html.H2(id=kpi_id, className="kpi-value"),
    ])

app.layout = html.Div(className="light", id="root", children=[
    dcc.Store(id="store-base", data=DF_BASE.to_json(orient="split")),
    dcc.Interval(id="auto-reload", interval=5*60*1000, n_intervals=0),  # 5min

    html.Div(className="container", children=[
        # Navbar
        html.Div(className="navbar", children=[
            html.Div(className="brand", children=[
                html.Div("📊", style={"fontSize":"20px"}),
                html.H1("Métricas de Veículos"),
                html.Span("v2.2 (Sheets)", className="badge"),
            ]),
            html.Div(className="actions", children=[
                dcc.RadioItems(
                    id="theme-toggle",
                    options=[{"label":"Claro","value":"light"},{"label":"Escuro","value":"dark"}],
                    value="light", inline=True,
                    inputStyle={"marginRight":"6px","marginLeft":"10px"}, style={"marginRight":"8px"},
                ),
                html.Button("Atualizar dados", id="btn-reload", n_clicks=0, className="btn ghost"),
                html.Button("Exportar Excel", id="btn-export-xlsx", n_clicks=0, className="btn"),
                html.Button("Exportar PDF", id="btn-export-pdf", n_clicks=0, className="btn"),
                dcc.Download(id="download-xlsx"),
                dcc.Download(id="download-pdf"),
                html.Div(id="export-msg", style={"marginLeft":"12px","fontSize":"12px"}),
            ]),
        ]),

        # Filtros
        html.Div(className="panel", children=[
            html.Div(className="filters", children=[
                html.Div(children=[
                    html.Div("Cidade", className="label"),
                    dcc.Dropdown(id="f_cidade", options=[], multi=True, placeholder="Selecione cidades…"),
                ]),
                html.Div(children=[
                    html.Div("Status", className="label"),
                    dcc.Dropdown(id="f_status", options=[], multi=True, placeholder="Selecione status…"),
                ]),
                html.Div(children=[
                    html.Div("Categoria", className="label"),
                    dcc.Dropdown(id="f_categoria", options=[], multi=True, placeholder="Selecione categorias…"),
                ]),
                html.Div(children=[
                    html.Div("Motivo", className="label"),
                    dcc.Dropdown(id="f_motivo", options=[], multi=True, placeholder="Selecione motivos…"),
                ]),
                html.Div(children=[
                    html.Div("Buscar por nome do site", className="label"),
                    dcc.Input(id="f_busca", type="text", placeholder="Digite parte do nome…", debounce=True),
                ]),
                html.Div(children=[
                    html.Div("Ordenação dos gráficos", className="label"),
                    dcc.RadioItems(
                        id="sort-order",
                        options=[{"label":"Decrescente","value":"desc"},{"label":"Crescente","value":"asc"}],
                        value="desc", inline=True,
                        inputStyle={"marginRight":"6px","marginLeft":"10px"}),
                ]),
            ]),
        ]),

        # KPIs
        html.Div(className="kpis", children=[
            kpi_card("kpi_total","Total de Veículos"),
            kpi_card("kpi_aprov","Aprovados"),
            kpi_card("kpi_reprov","Reprovados"),
            kpi_card("kpi_cidades","Cidades"),
        ]),

        # Gráficos
        html.Div(className="grid-2", children=[
            html.Div(className="card", children=[dcc.Graph(id="g_status", config={"displayModeBar":False})]),
            html.Div(className="card", children=[dcc.Graph(id="g_top_cidades", config={"displayModeBar":False})]),
        ]),
        html.Div(className="grid-2", children=[
            html.Div(className="card", children=[dcc.Graph(id="g_meses", config={"displayModeBar":False})]),
            html.Div(className="card", children=[dcc.Graph(id="g_top_sites", config={"displayModeBar":False})]),
        ]),

        # Lista
        html.Div(className="panel", children=[
            html.Div("Dados detalhados", className="label"),
            html.Div(className="card", children=[
                dash_table.DataTable(
                    id="tbl",
                    sort_action="native",
                    filter_action="native",
                    page_action="none",
                    style_table={"overflowX":"auto","maxWidth":"100%","height":"600px","overflowY":"auto"},
                    style_cell={
                        "padding":"10px","textAlign":"left","border":"0",
                        "whiteSpace":"normal","height":"auto","minWidth":"100px"},
                    style_header={"fontWeight":"700","border":"0"},
                    style_cell_conditional=[
                        {"if":{"column_id":"nome_do_veiculo"}, "minWidth":"260px","maxWidth":"480px"},
                        {"if":{"column_id":"cidade"}, "width":"160px","maxWidth":"200px"},
                        {"if":{"column_id":"status"}, "width":"140px","maxWidth":"160px"},
                        {"if":{"column_id":"motivo"}, "minWidth":"260px","maxWidth":"600px"},
                        {"if":{"column_id":"categoria"}, "width":"180px","maxWidth":"220px"},
                        {"if":{"column_id":"visualizacoes_junho"}, "textAlign":"right","width":"160px"},
                        {"if":{"column_id":"visualizacoes_julho"}, "textAlign":"right","width":"160px"},
                        {"if":{"column_id":"visualizacoes_agosto"}, "textAlign":"right","width":"160px"},
                    ],
                    style_data_conditional=[
                        {"if":{"filter_query":"{status} = REPROVADO","column_id":"status"},
                         "color":"#EF4444","fontWeight":"700"},
                        {"if":{"state":"active"}, "backgroundColor":"rgba(180,200,255,0.10)"},
                    ],
                ),
            ]),
        ]),
    ]),
])

# ========= LÓGICA =========
def _filtrar(base: pd.DataFrame, cidade, status, categoria, motivo, termo) -> pd.DataFrame:
    dff = base.copy()
    if cidade:    dff = dff[dff["cidade"].isin(cidade)]
    if status:    dff = dff[dff["status"].isin(status)]
    if categoria: dff = dff[dff["categoria"].isin(categoria)]
    if motivo:    dff = dff[dff["motivo"].isin(motivo)]
    if termo and str(termo).strip():
        alvo = "nome_fantasia" if "nome_fantasia" in dff.columns else \
               ("nome_do_veiculo" if "nome_do_veiculo" in dff.columns else dff.columns[0])
        dff = dff[dff[alvo].astype(str).str.contains(str(termo), case=False, na=False)]
    return dff

def _build_figures(dff: pd.DataFrame, order: str, theme: str):
    ascending = (order == "asc")
    # Status
    if "status" in dff and not dff.empty:
        g1 = dff["status"].astype(str).str.upper().value_counts().reset_index()
        g1.columns = ["status","qtd"]
        g1 = g1.sort_values("qtd", ascending=ascending)
        fig_status = px.bar(
            g1, x="status", y="qtd", text="qtd", title="Distribuição por Status",
            color="status",
            color_discrete_map={
                "APROVADO":"#22C55E","REPROVADO":"#EF4444",
                "APROVADO PARCIAL":"#F59E0B","PENDENTE":"#A78BFA","INSTA":"#06B6D4",
            },
        )
        fig_status.update_traces(textposition="outside")
        fig_status.update_layout(xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()))
    else:
        fig_status = px.bar(title="Distribuição por Status")
    style_fig(fig_status, theme)

    # Cidades
    if "cidade" in dff and not dff.empty:
        base_cid = dff["cidade"].value_counts().reset_index()
        base_cid.columns = ["cidade","qtd"]
        base_cid = base_cid.sort_values("qtd", ascending=False).head(10)
        base_cid = base_cid.sort_values("qtd", ascending=ascending)
        seq = get_sequence(theme, len(base_cid))
        fig_cidades = px.bar(base_cid, x="cidade", y="qtd", text="qtd", title="Top 10 Cidades",
                             color="cidade", color_discrete_sequence=seq)
        fig_cidades.update_traces(textposition="outside")
        fig_cidades.update_layout(showlegend=False,
                                  xaxis=dict(categoryorder="array", categoryarray=base_cid["cidade"].tolist()))
    else:
        fig_cidades = px.bar(title="Top 10 Cidades")
    style_fig(fig_cidades, theme)

    # Meses
    vj = float(dff["visualizacoes_junho"].sum()) if "visualizacoes_junho" in dff else 0.0
    vl = float(dff["visualizacoes_julho"].sum()) if "visualizacoes_julho" in dff else 0.0
    va = float(dff["visualizacoes_agosto"].sum()) if "visualizacoes_agosto" in dff else 0.0
    g3 = pd.DataFrame({"Mês":["Junho","Julho","Agosto"],
                       "Visualizações":[vj,vl,va]}).sort_values("Visualizações", ascending=ascending)
    seq3 = get_sequence(theme, len(g3))
    fig_meses = px.bar(g3, x="Mês", y="Visualizações", text="Visualizações",
                       title="Total de Visualizações por Mês", color="Mês",
                       color_discrete_sequence=seq3)
    fig_meses.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig_meses.update_layout(showlegend=False,
                            xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()))
    style_fig(fig_meses, theme)

    # Sites
    if {"nome_fantasia","total_visualizacoes"}.issubset(dff.columns) and not dff.empty:
        g4 = dff.nlargest(10,"total_visualizacoes")[["nome_fantasia","total_visualizacoes"]]
        g4 = g4.sort_values("total_visualizacoes", ascending=ascending)
        seq4 = get_sequence(theme, len(g4))
        fig_sites = px.bar(g4, x="total_visualizacoes", y="nome_fantasia", orientation="h",
                           text="total_visualizacoes", title="Top 10 Sites (Total de Visualizações)",
                           color="nome_fantasia", color_discrete_sequence=seq4)
        fig_sites.update_traces(texttemplate="%{text:.0f}")
        fig_sites.update_layout(showlegend=False,
                                yaxis=dict(categoryorder="array", categoryarray=g4["nome_fantasia"].tolist()))
    else:
        fig_sites = px.bar(title="Top 10 Sites (Total de Visualizações)")
    style_fig(fig_sites, theme)

    return fig_status, fig_cidades, fig_meses, fig_sites

# ========= CALLBACKS =========
# Atualiza dataset (Sheets) no clique e no timer
@app.callback(
    Output("store-base", "data"),
    Output("export-msg", "children", allow_duplicate=True),
    Input("btn-reload", "n_clicks"),
    Input("auto-reload", "n_intervals"),
    prevent_initial_call=True,
)
def do_reload(_n, _t):
    try:
        df = load_data()
        msg = f"Dados atualizados: {len(df):,} linhas às {datetime.now():%H:%M:%S}".replace(",", ".")
        return df.to_json(orient="split"), msg
    except Exception as e:
        return no_update, f"Falha ao atualizar dados: {e}"

# Troca tema
@app.callback(Output("root","className"), Input("theme-toggle","value"))
def set_theme(theme): return "light" if theme=="light" else "dark"

# Atualiza filtros/visuais/lista quando dados ou filtros mudarem
@app.callback(
    Output("f_cidade", "options"),
    Output("f_status", "options"),
    Output("f_categoria", "options"),
    Output("f_motivo", "options"),
    Output("kpi_total","children"),
    Output("kpi_aprov","children"),
    Output("kpi_reprov","children"),
    Output("kpi_cidades","children"),
    Output("g_status","figure"),
    Output("g_top_cidades","figure"),
    Output("g_meses","figure"),
    Output("g_top_sites","figure"),
    Output("tbl","data"),
    Output("tbl","columns"),
    Input("store-base","data"),
    Input("f_cidade","value"),
    Input("f_status","value"),
    Input("f_categoria","value"),
    Input("f_motivo","value"),
    Input("f_busca","value"),
    Input("sort-order","value"),
    State("theme-toggle","value"),
)
def atualizar(store_json, f_cidade, f_status, f_categoria, f_motivo, f_busca, order, theme):
    base = pd.read_json(store_json, orient="split") if store_json else DF_BASE

    cidade_opts    = [{"label": c, "value": c} for c in sorted(base.get("cidade", pd.Series()).dropna().unique())]
    status_opts    = [{"label": s, "value": s} for s in sorted(base.get("status", pd.Series()).dropna().unique())]
    categoria_opts = [{"label": c, "value": c} for c in sorted(base.get("categoria", pd.Series()).dropna().unique())]
    motivo_opts    = [{"label": m, "value": m} for m in sorted(base.get("motivo", pd.Series()).dropna().unique()) if m]

    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_motivo, f_busca)
    fig_status, fig_cidades, fig_meses, fig_sites = _build_figures(dff, order, theme)

    total = int(len(dff))
    aprov = int((dff["status"]=="APROVADO").sum()) if "status" in dff else 0
    reprov = int((dff["status"]=="REPROVADO").sum()) if "status" in dff else 0
    cidades_qtd = int(dff["cidade"].nunique()) if "cidade" in dff else 0

    cols_order = ["nome_do_veiculo","cidade","status","motivo",
                  "categoria","visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]
    friendly = {
        "nome_do_veiculo":"Nome do Veículo","cidade":"Cidade","status":"Status","motivo":"Motivo",
        "categoria":"Categoria","visualizacoes_junho":"Visualizações Junho",
        "visualizacoes_julho":"Visualizações Julho","visualizacoes_agosto":"Visualizações Agosto",
    }
    present = [c for c in cols_order if c in dff.columns]
    data = dff[present].to_dict("records")
    columns = [{"name": friendly[c], "id": c} for c in present]

    return (cidade_opts, status_opts, categoria_opts, motivo_opts,
            f"{total}", f"{aprov}", f"{reprov}", f"{cidades_qtd}",
            fig_status, fig_cidades, fig_meses, fig_sites,
            data, columns)

# ========= EXPORTAÇÕES =========
def build_xlsx_bytes(dff: pd.DataFrame) -> bytes:
    import xlsxwriter
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        dff.to_excel(writer, sheet_name="Dados", index=False)
        wb = writer.book; ws = writer.sheets["Dados"]
        fmt_text = wb.add_format({"font_name":"Segoe UI","font_size":10})
        fmt_header = wb.add_format({"bold":True,"bg_color":"#1F2937","font_color":"#FFFFFF"})
        fmt_num = wb.add_format({"num_format":"#,##0","align":"right","font_name":"Segoe UI"})
        for col_idx, col_name in enumerate(dff.columns):
            ws.write(0, col_idx, col_name, fmt_header)
            ws.set_column(col_idx, col_idx, 22, fmt_text)
        for col in ["visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto","total_visualizacoes"]:
            if col in dff.columns:
                idx = dff.columns.get_loc(col)
                ws.set_column(idx, idx, 18, fmt_num)
    out.seek(0)
    return out.getvalue()

@app.callback(
    Output("download-xlsx","data"),
    Output("export-msg","children", allow_duplicate=True),
    Input("btn-export-xlsx","n_clicks"),
    State("store-base","data"),
    State("f_cidade","value"),
    State("f_status","value"),
    State("f_categoria","value"),
    State("f_motivo","value"),
    State("f_busca","value"),
    prevent_initial_call=True,
)
def exportar_xlsx(n, store_json, f_cidade, f_status, f_categoria, f_motivo, f_busca):
    base = pd.read_json(store_json, orient="split") if store_json else DF_BASE
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_motivo, f_busca)
    cols_export = ["nome_do_veiculo","cidade","status","motivo",
                   "categoria","visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto",
                   "total_visualizacoes"]
    cols_export = [c for c in cols_export if c in dff.columns]
    xbytes = build_xlsx_bytes(dff[cols_export].copy())
    return send_bytes(lambda b: b.write(xbytes), filename="dados_observatorio.xlsx"), "Exportado em Excel."

def build_pdf_bytes(dff: pd.DataFrame, figs: list, theme: str) -> bytes:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, LongTable, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                            leftMargin=24, rightMargin=24, topMargin=24, bottomMargin=24)
    styles = getSampleStyleSheet()
    style_h1 = styles["Heading1"]; style_h1.textColor = "#0F172A" if theme=="light" else "#E6ECFF"
    style_h2 = styles["Heading2"]; style_p = styles["Normal"]

    story = []
    story.append(Paragraph("Observatório – Métricas de Veículos", style_h1))
    story.append(Paragraph(datetime.now().strftime("%d/%m/%Y %H:%M"), style_p))
    story.append(Spacer(1, 10))

    for title, fig in figs:
        try:
            png = fig.to_image(format="png", scale=2)  # requer kaleido
            story.append(Paragraph(title, style_h2))
            story.append(Image(io.BytesIO(png), width=500, height=300))
            story.append(Spacer(1, 12))
        except Exception as e:
            story.append(Paragraph(f"Falha ao renderizar gráfico: {e}", style_p))

    friendly = {
        "nome_do_veiculo":"Nome do Veículo","cidade":"Cidade","status":"Status","motivo":"Motivo",
        "categoria":"Categoria","visualizacoes_junho":"Visualizações Junho",
        "visualizacoes_julho":"Visualizações Julho","visualizacoes_agosto":"Visualizações Agosto",
    }
    present = [c for c in ["nome_do_veiculo","cidade","status","motivo",
                           "categoria","visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]
               if c in dff.columns]

    weights = {"nome_do_veiculo":0.28,"cidade":0.12,"status":0.10,"motivo":0.28,
               "categoria":0.10,"visualizacoes_junho":0.04,"visualizacoes_julho":0.04,"visualizacoes_agosto":0.04}
    total_w = sum(weights.get(c, 0.1) for c in present)
    col_widths = [max(40, doc.width * (weights.get(c, 0.1)/total_w)) for c in present]

    cell = ParagraphStyle("cell", fontName="Helvetica", fontSize=8, leading=10)
    cell_b = ParagraphStyle("cell_b", parent=cell, fontName="Helvetica-Bold")

    header = [Paragraph(friendly[c], cell_b) for c in present]
    data = [header]
    numeric_cols = {"visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"}

    df_show = dff[present].fillna("")
    for _, row in df_show.iterrows():
        r = []
        for c in present:
            v = row[c]
            if c in numeric_cols:
                try:
                    v = int(float(v))
                    r.append(f"{v:,}".replace(",", "."))
                except Exception:
                    r.append(str(v))
            else:
                r.append(Paragraph(str(v), cell))
        data.append(r)

    from reportlab.platypus import TableStyle
    tbl = LongTable(data, repeatRows=1, colWidths=col_widths)
    ts = TableStyle([
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#1F2937")),
        ("TEXTCOLOR",(0,0),(-1,0), colors.white),
        ("FONTNAME",(0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 8),
        ("VALIGN",(0,0),(-1,-1), "TOP"),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.HexColor("#F8FAFC"), colors.HexColor("#EEF2F7")]),
        ("INNERGRID",(0,0),(-1,-1), 0.25, colors.HexColor("#CBD5E1")),
        ("BOX",(0,0),(-1,-1), 0.25, colors.HexColor("#CBD5E1")),
        ("LEFTPADDING",(0,0),(-1,-1), 4),
        ("RIGHTPADDING",(0,0),(-1,-1), 4),
        ("TOPPADDING",(0,0),(-1,-1), 2),
        ("BOTTOMPADDING",(0,0),(-1,-1), 2),
    ])
    for idx, col in enumerate(present):
        if col in numeric_cols:
            ts.add("ALIGN", (idx,1), (idx,-1), "RIGHT")
    tbl.setStyle(ts)

    from reportlab.platypus import PageBreak, Spacer
    story.append(PageBreak())
    story.append(Paragraph("Dados detalhados", style_h2))
    story.append(Spacer(1, 6))
    story.append(tbl)

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()

@app.callback(
    Output("download-pdf","data"),
    Output("export-msg","children", allow_duplicate=True),
    Input("btn-export-pdf","n_clicks"),
    State("store-base","data"),
    State("f_cidade","value"),
    State("f_status","value"),
    State("f_categoria","value"),
    State("f_motivo","value"),
    State("f_busca","value"),
    State("sort-order","value"),
    State("theme-toggle","value"),
    prevent_initial_call=True,
)
def exportar_pdf(n, store_json, f_cidade, f_status, f_categoria, f_motivo, f_busca, order, theme):
    base = pd.read_json(store_json, orient="split") if store_json else DF_BASE
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_motivo, f_busca)
    fig_status, fig_cidades, fig_meses, fig_sites = _build_figures(dff, order, theme)
    try:
        payload = build_pdf_bytes(
            dff=dff[[c for c in ["nome_do_veiculo","cidade","status","motivo",
                                 "categoria","visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]
                     if c in dff.columns]],
            figs=[
                ("Distribuição por Status", fig_status),
                ("Top 10 Cidades", fig_cidades),
                ("Total de Visualizações por Mês", fig_meses),
                ("Top 10 Sites (Total de Visualizações)", fig_sites),
            ],
            theme=theme,
        )
    except Exception as e:
        return no_update, f"Falha ao exportar PDF: {e}. Instale 'kaleido' e 'reportlab'."
    return send_bytes(lambda b: b.write(payload), filename="dados_observatorio.pdf"), "Exportado em PDF."

# ========= RUN =========
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    try:
        app.run(debug=True, host="0.0.0.0", port=port)
    except AttributeError:
        app.run_server(debug=True, host="0.0.0.0", port=port)
