# -*- coding: utf-8 -*-
"""
Métricas de Veículos – v2.5 (Sheets/Excel)
- Botão ATUALIZAR busca sempre a planilha do Google e atualiza tudo (incl. Dados detalhados).
- A URL do Sheets já vem embutida como padrão; pode ser sobrescrita via env SHEETS_CSV_URL.
- Cache-buster + timeout/retry curtos (evita travamentos).
- Filtros: cidade/status/categoria/motivo/busca + ordenação.
- KPIs, gráficos, tabela (lista única) e exportar Excel.
"""

import os, io, time, unicodedata
import pandas as pd
import plotly.express as px

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dash import Dash, dcc, html, dash_table, Input, Output, State, no_update
from dash.dcc import send_bytes

# ====================== CONFIG ======================
# URL padrão -> sua planilha (pode sobrescrever com env SHEETS_CSV_URL)
SHEETS_CSV_URL = os.getenv(
    "SHEETS_CSV_URL",
    "https://docs.google.com/spreadsheets/d/17TnGB6NpsziDec4fPH-d0TCQwk2LN0BAv6yjmIpyZnI/edit?gid=1225239898#gid=1225239898"
).strip()

LOCAL_XLSX     = os.getenv("LOCAL_XLSX", "Recadastramento (respostas).xlsx")
REFRESH_MIN    = int(os.getenv("REFRESH_MINUTES", "0") or 0)  # auto-refresh (0 = desliga)

# Sessão HTTP com retry leve
session = requests.Session()
_retry = Retry(
    total=2, read=2, connect=2, backoff_factor=0.3,
    status_forcelist=[408, 429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
session.mount("https://", HTTPAdapter(max_retries=_retry))
session.mount("http://",  HTTPAdapter(max_retries=_retry))


# ====================== HELPERS ======================
def _normalize(colname: str) -> str:
    s = str(colname).strip().replace("\n", " ")
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    return s.lower().replace(" ", "_")


def _to_export_csv_url(url: str) -> str:
    """Converte /edit?...#gid=xxx para /export?format=csv&gid=xxx."""
    if "/export" in url:
        return url
    if "/edit" in url and "gid=" in url:
        gid = url.split("gid=")[-1].split("&")[0].split("#")[0]
        base = url.split("/edit")[0]
        return f"{base}/export?format=csv&gid={gid}"
    return url


def _read_google_csv(url: str) -> pd.DataFrame:
    """Lê CSV do Sheets: timeout curto + cache-buster (não trava a UI)."""
    url = _to_export_csv_url(url)
    ts  = int(time.time())
    sep = "&" if "?" in url else "?"
    final = f"{url}{sep}cachebust={ts}"

    r = session.get(
        final,
        timeout=(5, 12),  # connect, read
        headers={"Cache-Control":"no-cache","Pragma":"no-cache","User-Agent":"DashApp/Render"},
        allow_redirects=True,
    )
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content), dtype=str, encoding="utf-8")


def clean_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace(["", "nan", "None", "NaN"], "0")
    s = s.str.replace(r"[^0-9,.\-]", "", regex=True)
    both = s.str.contains(",") & s.str.contains(r"\.")
    s = s.where(~both, s.str.replace(".", "", regex=False))
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _pick_motivo_column(cols: list[str]) -> str | None:
    preferidos = [
        "motivo", "motivo_da_reprovacao", "motivo_reprovacao",
        "motivo_do_status", "motivo_reprovado", "motivo_do_indeferimento"
    ]
    for c in preferidos:
        if c in cols: return c
    cand = [c for c in cols if "motivo" in c]
    if cand:
        reforco = [c for c in cand if ("reprov" in c or "indefer" in c)]
        return reforco[0] if reforco else cand[0]
    outras = [c for c in cols if any(k in c for k in ["reprov", "indefer", "observa", "justific", "coment"])]
    return outras[0] if outras else None


def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize(c) for c in df.columns]

    ren = {
        "nome_do_veiculo.": "nome_fantasia",
        "nome_empresarial_da_empresa_responsavel.": "razao_social",
        "endereco_no_site": "endereco_site",
        "url_ativa_do_veiculo.": "url",
        "total_de_visualizacoes_junho":  "visualizacoes_junho",
        "total_de_vizualizacoes_junho":  "visualizacoes_junho",
        "total_de_visualizacoes_julho":  "visualizacoes_julho",
        "total_de_vizualizacoes_julho":  "visualizacoes_julho",
        "total_de_visualizacoes_agosto": "visualizacoes_agosto",
        "total_de_vizualizacoes_agosto": "visualizacoes_agosto",
    }
    df = df.rename(columns=ren)

    for c in ["visualizacoes_junho", "visualizacoes_julho", "visualizacoes_agosto"]:
        if c not in df.columns: df[c] = 0
        df[c] = clean_numeric(df[c])

    df["total_visualizacoes"] = df[["visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]].sum(axis=1)

    for c in ["cidade", "categoria", "status"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        else:
            df[c] = "Não informado"

    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.upper()

    if "nome_fantasia" not in df.columns:
        df["nome_fantasia"] = ""
    df["nome_do_veiculo"] = df["nome_fantasia"].astype(str)
    if "url" not in df.columns:
        df["url"] = ""

    motivo_col = _pick_motivo_column(list(df.columns))
    if motivo_col and motivo_col in df.columns:
        df["motivo"] = df[motivo_col].astype(str).fillna("").str.strip()
    else:
        df["motivo"] = ""
    return df


def load_data_from_source(prefer_google=True) -> tuple[pd.DataFrame, str]:
    """
    Tenta Sheets (prefer_google=True) e cai para Excel local; devolve (df, source).
    """
    if prefer_google and SHEETS_CSV_URL:
        try:
            raw = _read_google_csv(SHEETS_CSV_URL)
            return _prepare_df(raw), "sheets"
        except Exception as e:
            print("[WARN] Sheets falhou:", e)

    if os.path.exists(LOCAL_XLSX):
        base = pd.read_excel(LOCAL_XLSX)
        return _prepare_df(base), "excel"

    return _prepare_df(pd.DataFrame()), "none"


# ====================== CORES / ESTILO FIG ======================
THEME = {
    "light": {
        "font": "#0F172A","grid": "#E9EDF5","paper":"rgba(0,0,0,0)","plot":"rgba(0,0,0,0)",
        "template":"plotly_white",
        "seq":["#3B82F6","#22C55E","#F59E0B","#EF4444","#06B6D4","#A78BFA","#10B981","#F43F5E","#8B5CF6","#14B8A6"]
    },
    "dark": {
        "font": "#E6ECFF","grid": "#23324B","paper":"rgba(0,0,0,0)","plot":"rgba(0,0,0,0)",
        "template":"plotly_dark",
        "seq":["#60A5FA","#34D399","#FBBF24","#F87171","#22D3EE","#CABFFD","#4ADE80","#FB7185","#A78BFA","#2DD4BF"]
    },
}
def style_fig(fig, theme="light"):
    c = THEME[theme]
    fig.update_layout(template=c["template"], paper_bgcolor=c["paper"], plot_bgcolor=c["plot"],
                      font=dict(color=c["font"], size=13), margin=dict(l=12,r=12,t=48,b=12))
    fig.update_xaxes(gridcolor=c["grid"], zerolinecolor=c["grid"], title_font_color=c["font"], tickfont_color=c["font"])
    fig.update_yaxes(gridcolor=c["grid"], zerolinecolor=c["grid"], title_font_color=c["font"], tickfont_color=c["font"])
    return fig


# ====================== APP / LAYOUT ======================
DF_BASE, _SRC = load_data_from_source(prefer_google=True)

app = Dash(__name__, title="Métricas de Veículos")
server = app.server

def kpi(label, kpi_id):
    return html.Div(className="card kpi", children=[html.P(label, className="kpi-title"), html.H2(id=kpi_id)])

app.layout = html.Div(id="root", className="light", children=[
    dcc.Store(id="store-base", data=DF_BASE.to_json(orient="split")),
    dcc.Interval(id="auto-refresh", interval=max(REFRESH_MIN,0)*60*1000, disabled=(REFRESH_MIN<=0)),

    html.Div(className="container", children=[

        # NAVBAR
        html.Div(className="navbar", children=[
            html.Div(className="brand", children=[
                html.Div("📊", style={"fontSize":"20px"}),
                html.H1("Métricas de Veículos"),
                html.Span("v2.5 (Sheets/Excel)", className="badge"),
            ]),
            html.Div(className="actions", children=[
                dcc.RadioItems(
                    id="theme-toggle",
                    options=[{"label":"Claro","value":"light"},{"label":"Escuro","value":"dark"}],
                    value="light", inline=True, inputStyle={"marginLeft":"12px","marginRight":"6px"}
                ),
                html.Button("Atualizar dados", id="btn-reload", className="btn ghost"),
                html.Button("Exportar Excel", id="btn-xlsx", className="btn"),
                dcc.Download(id="download-xlsx"),
                html.Div(id="export-msg", className="hint")
            ])
        ]),

        # FILTROS
        html.Div(className="panel", children=[
            html.Div(className="filters", children=[
                html.Div(children=[html.Div("Cidade", className="label"),
                    dcc.Dropdown(id="f_cidade",
                                 options=[{"label":c, "value":c} for c in sorted(DF_BASE["cidade"].dropna().unique())],
                                 multi=True, placeholder="Selecione cidades…")]),
                html.Div(children=[html.Div("Status", className="label"),
                    dcc.Dropdown(id="f_status",
                                 options=[{"label":s, "value":s} for s in sorted(DF_BASE["status"].dropna().unique())],
                                 multi=True, placeholder="Selecione status…")]),
                html.Div(children=[html.Div("Categoria", className="label"),
                    dcc.Dropdown(id="f_categoria",
                                 options=[{"label":c, "value":c} for c in sorted(DF_BASE["categoria"].dropna().unique())],
                                 multi=True, placeholder="Selecione categorias…")]),
                html.Div(children=[html.Div("Motivo", className="label"),
                    dcc.Dropdown(id="f_motivo",
                                 options=[{"label":m, "value":m} for m in sorted(DF_BASE["motivo"].dropna().unique()) if m],
                                 multi=True, placeholder="Selecione motivos…")]),
                html.Div(children=[html.Div("Buscar por nome do site", className="label"),
                    dcc.Input(id="f_busca", type="text", debounce=True, placeholder="Digite parte do nome…")]),
                html.Div(children=[html.Div("Ordenação dos gráficos", className="label"),
                    dcc.RadioItems(id="sort-order", value="desc", inline=True,
                                   options=[{"label":"Decrescente","value":"desc"},{"label":"Crescente","value":"asc"}],
                                   inputStyle={"marginLeft":"12px","marginRight":"6px"})]),
            ])
        ]),

        # KPIs
        html.Div(className="kpis", children=[
            kpi("Total de Veículos","kpi_total"),
            kpi("Aprovados","kpi_aprov"),
            kpi("Reprovados","kpi_reprov"),
            kpi("Cidades","kpi_cidades"),
        ]),

        # GRÁFICOS
        html.Div(className="grid-2", children=[
            html.Div(className="card", children=[dcc.Graph(id="g_status", config={"displayModeBar":False})]),
            html.Div(className="card", children=[dcc.Graph(id="g_top_cidades", config={"displayModeBar":False})]),
        ]),
        html.Div(className="grid-2", children=[
            html.Div(className="card", children=[dcc.Graph(id="g_meses", config={"displayModeBar":False})]),
            html.Div(className="card", children=[dcc.Graph(id="g_top_sites", config={"displayModeBar":False})]),
        ]),

        # TABELA
        html.Div(className="panel", children=[
            html.Div("Dados detalhados", className="label"),
            html.Div(className="card", children=[
                dash_table.DataTable(
                    id="tbl", page_size=9999,  # lista única
                    sort_action="native", filter_action="native",
                    style_table={"overflowX":"auto", "maxWidth":"100%"},
                    style_cell={"padding":"10px","textAlign":"left","border":"0","whiteSpace":"normal","height":"auto"},
                    style_header={"fontWeight":"700","border":"0"},
                )
            ])
        ]),
    ])
])


# ====================== CALLBACKS ======================
@app.callback(Output("root","className"), Input("theme-toggle","value"))
def set_theme_class(t):
    return "dark" if t == "dark" else "light"


# Quando store muda (ex.: após atualizar), recarrega OPTIONS dos filtros
@app.callback(
    Output("f_cidade","options"),
    Output("f_status","options"),
    Output("f_categoria","options"),
    Output("f_motivo","options"),
    Input("store-base","data"),
)
def sync_filter_options(store_json):
    df = pd.read_json(store_json, orient="split")
    cidade   = [{"label":c,"value":c} for c in sorted(df["cidade"].dropna().unique())]
    status   = [{"label":s,"value":s} for s in sorted(df["status"].dropna().unique())]
    categoria= [{"label":c,"value":c} for c in sorted(df["categoria"].dropna().unique())]
    motivo   = [{"label":m,"value":m} for m in sorted(df["motivo"].dropna().unique()) if m]
    return cidade, status, categoria, motivo


# Botão ATUALIZAR e Auto-refresh -> carregam SEMPRE da planilha (fallback Excel)
@app.callback(
    Output("store-base","data"),
    Output("export-msg","children"),
    Input("btn-reload","n_clicks"),
    Input("auto-refresh","n_intervals"),
    prevent_initial_call=True,
)
def do_refresh(_btn, _interval):
    try:
        # prefer_google=True garante que o botão tenta planilha primeiro
        df, src = load_data_from_source(prefer_google=True)
        src_txt = "Google Sheets" if src == "sheets" else ("Excel local" if src == "excel" else "vazio")
        return df.to_json(orient="split"), f"Dados atualizados de {src_txt} às {time.strftime('%H:%M:%S')}."
    except Exception as e:
        return no_update, f"Falha ao atualizar: {e}"


def _filtrar(base: pd.DataFrame, cidade, status, categoria, motivo, termo) -> pd.DataFrame:
    dff = base.copy()
    if cidade:    dff = dff[dff["cidade"].isin(cidade)]
    if status:    dff = dff[dff["status"].isin(status)]
    if categoria: dff = dff[dff["categoria"].isin(categoria)]
    if motivo:    dff = dff[dff["motivo"].isin(motivo)]
    if termo and str(termo).strip():
        alvo = "nome_do_veiculo" if "nome_do_veiculo" in dff.columns else dff.columns[0]
        dff = dff[dff[alvo].astype(str).str.contains(str(termo), case=False, na=False)]
    return dff


@app.callback(
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
    base = pd.read_json(store_json, orient="split")
    dff  = _filtrar(base, f_cidade, f_status, f_categoria, f_motivo, f_busca)
    asc  = (order == "asc")

    total  = int(len(dff))
    aprov  = int((dff["status"]=="APROVADO").sum()) if "status" in dff else 0
    reprov = int((dff["status"]=="REPROVADO").sum()) if "status" in dff else 0
    cidades= int(dff["cidade"].nunique()) if "cidade" in dff else 0

    # Status
    if "status" in dff and not dff.empty:
        g1 = dff["status"].value_counts().reset_index()
        g1.columns=["status","qtd"]
        g1 = g1.sort_values("qtd", ascending=asc)
        fig1 = px.bar(g1, x="status", y="qtd", text="qtd", title="Distribuição por Status",
                      color="status",
                      color_discrete_map={
                          "APROVADO":"#22C55E","REPROVADO":"#EF4444","APROVADO PARCIAL":"#F59E0B","PENDENTE":"#A78BFA"
                      })
        fig1.update_traces(textposition="outside")
        fig1.update_layout(showlegend=True, xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()))
    else:
        fig1 = px.bar(title="Distribuição por Status")
    style_fig(fig1, theme)

    # Top cidades
    if "cidade" in dff and not dff.empty:
        g2 = dff["cidade"].value_counts().reset_index()
        g2.columns=["cidade","qtd"]
        g2 = g2.sort_values("qtd", ascending=False).head(10).sort_values("qtd", ascending=asc)
        fig2 = px.bar(g2, x="cidade", y="qtd", text="qtd", title="Top 10 Cidades",
                      color="cidade", color_discrete_sequence=THEME[theme]["seq"])
        fig2.update_traces(textposition="outside")
        fig2.update_layout(showlegend=False, xaxis=dict(categoryorder="array", categoryarray=g2["cidade"].tolist()))
    else:
        fig2 = px.bar(title="Top 10 Cidades")
    style_fig(fig2, theme)

    # Visualizações por mês
    vj = float(dff["visualizacoes_junho"].sum())  if "visualizacoes_junho" in dff else 0.0
    vl = float(dff["visualizacoes_julho"].sum())  if "visualizacoes_julho" in dff else 0.0
    va = float(dff["visualizacoes_agosto"].sum()) if "visualizacoes_agosto" in dff else 0.0
    g3 = pd.DataFrame({"Mês":["Junho","Julho","Agosto"], "Visualizações":[vj,vl,va]}).sort_values("Visualizações", ascending=asc)
    fig3 = px.bar(g3, x="Mês", y="Visualizações", text="Visualizações", title="Total de Visualizações por Mês",
                  color="Mês", color_discrete_sequence=THEME[theme]["seq"])
    fig3.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig3.update_layout(showlegend=False, xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()))
    style_fig(fig3, theme)

    # Top sites
    if {"nome_do_veiculo","total_visualizacoes"}.issubset(dff.columns) and not dff.empty:
        g4 = dff.nlargest(10, "total_visualizacoes")[["nome_do_veiculo","total_visualizacoes"]]
        g4 = g4.sort_values("total_visualizacoes", ascending=asc)
        fig4 = px.bar(g4, x="total_visualizacoes", y="nome_do_veiculo", orientation="h",
                      text="total_visualizacoes", title="Top 10 Sites (Total de Visualizações)",
                      color="nome_do_veiculo", color_discrete_sequence=THEME[theme]["seq"])
        fig4.update_traces(texttemplate="%{text:.0f}")
        fig4.update_layout(showlegend=False, yaxis=dict(categoryorder="array", categoryarray=g4["nome_do_veiculo"].tolist()))
    else:
        fig4 = px.bar(title="Top 10 Sites (Total de Visualizações)")
    style_fig(fig4, theme)

    # Tabela (lista inteira atualiza junto com o Store)
    cols = ["nome_do_veiculo","cidade","status","motivo","categoria",
            "visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto"]
    present = [c for c in cols if c in dff.columns]
    friendly = {
        "nome_do_veiculo":"Nome do Veículo", "cidade":"Cidade", "status":"Status", "motivo":"Motivo",
        "categoria":"Categoria", "visualizacoes_junho":"Visualizações Junho",
        "visualizacoes_julho":"Visualizações Julho", "visualizacoes_agosto":"Visualizações Agosto",
    }
    data = dff[present].to_dict("records")
    columns = [{"name":friendly.get(c,c), "id":c} for c in present]

    return f"{total}", f"{aprov}", f"{reprov}", f"{cidades}", fig1, fig2, fig3, fig4, data, columns


# Exportar Excel
@app.callback(
    Output("download-xlsx","data"),
    Input("btn-xlsx","n_clicks"),
    State("store-base","data"),
    State("f_cidade","value"),
    State("f_status","value"),
    State("f_categoria","value"),
    State("f_motivo","value"),
    State("f_busca","value"),
    prevent_initial_call=True,
)
def export_xlsx(_n, store_json, fc, fs, fcat, fm, busca):
    df = pd.read_json(store_json, orient="split")
    # Mesmo filtro da tela para exportar somente o que está vendo
    def apply_filters(base):
        dff = base.copy()
        if fc:   dff = dff[dff["cidade"].isin(fc)]
        if fs:   dff = dff[dff["status"].isin(fs)]
        if fcat: dff = dff[dff["categoria"].isin(fcat)]
        if fm:   dff = dff[dff["motivo"].isin(fm)]
        if busca and str(busca).strip():
            alvo = "nome_do_veiculo" if "nome_do_veiculo" in dff.columns else dff.columns[0]
            dff = dff[dff[alvo].astype(str).str.contains(str(busca), case=False, na=False)]
        return dff
    dff = apply_filters(df)

    cols = ["nome_do_veiculo","cidade","status","motivo","categoria",
            "visualizacoes_junho","visualizacoes_julho","visualizacoes_agosto","total_visualizacoes"]
    cols = [c for c in cols if c in dff.columns]

    def _to_bytes(b):
        with pd.ExcelWriter(b, engine="xlsxwriter") as writer:
            dff[cols].to_excel(writer, index=False, sheet_name="dados")
        b.seek(0);  return b.read()

    return send_bytes(_to_bytes, filename="dados_observatorio.xlsx")


# ====================== RUN ======================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8050"))
    try:
        app.run(debug=True, host="0.0.0.0", port=port)
    except AttributeError:
        app.run_server(debug=True, host="0.0.0.0", port=port)
