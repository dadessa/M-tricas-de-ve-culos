# -*- coding: utf-8 -*-
"""
Métricas de Veículos (v3.8.2)
- FIX: Pipeline de dados mais resiliente (Google Sheets CSV / Excel local)
  * Novas rotas de leitura com múltiplas tentativas, encodings e engines
  * Suporte a URL do Google Sheets construída via SHEETS_GSHEET_ID + SHEETS_GID
  * Opção de forçar fonte local via FORCE_LOCAL_DATA=1
  * Validação mínima do dataset (tem linhas/colunas) antes de aceitar
  * Fallback garantido para DataFrame vazio SEM quebrar o app
- Persistência de valores (Valor Planejado / Valor Pago) em JSON (inalterado)
- Filtro “Somente Valor Pago ≠ 0” (inalterado)
- Meses Jul/Ago/Set + Média Trimestral; Top10 por Média (inalterado)
- Tabela contínua e exports atualizados (inalterado)
"""

import os
import json
import time
import unicodedata
from io import BytesIO
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import Dash, dcc, html, dash_table
from dash import Input, Output, State
from dash.dash_table.Format import Format, Group, Scheme

# ========= CONFIG / PERSISTÊNCIA =========
EXCEL_PATH = os.getenv("EXCEL_PATH", "Recadastramento (respostas).xlsx")  # fallback local (dev)
SHEETS_CSV_URL = os.getenv("SHEETS_CSV_URL")                              # CSV público direto
SHEETS_GSHEET_ID = os.getenv("SHEETS_GSHEET_ID")                          # id do arquivo do Sheets
SHEETS_GID = os.getenv("SHEETS_GID")                                      # gid da aba
FORCE_LOCAL_DATA = os.getenv("FORCE_LOCAL_DATA", "0") == "1"              # força carregar apenas do Excel
VALORES_JSON_PATH = os.getenv("VALORES_JSON_PATH", "valores_financeiros.json")

# ========= LOG UTILS =========
def _log(*args):
    print("[dashboard]", *args)

# Lock simples para I/O (entre threads do mesmo worker)
try:
    import threading
    _MEM_LOCK = threading.Lock()
except Exception:
    _MEM_LOCK = None

def _acquire_file_lock(fp):
    """Tenta bloquear arquivo com fcntl (Unix). Ignora em Windows/indisponível."""
    try:
        import fcntl
        fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        return True
    except Exception:
        return False

def _release_file_lock(fp):
    try:
        import fcntl
        fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass

def _safe_dir_for(path: str):
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _read_persisted_vals() -> dict:
    """Lê JSON persistido; formato: {nome_do_veiculo: {valor_planejado: float, valor_pago: float}}"""
    path = VALORES_JSON_PATH
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            _acquire_file_lock(f)
            data = json.load(f)
            _release_file_lock(f)
        if isinstance(data, dict):
            return data
        return {}
    except Exception as e:
        _log("[persist] Falha ao ler JSON:", e)
        return {}

def _write_persisted_vals(data: dict) -> None:
    """Escrita atômica: arquivo temporário + replace."""
    path = VALORES_JSON_PATH
    try:
        _safe_dir_for(path)
        tmp = f"{path}.tmp"
        if _MEM_LOCK:
            _MEM_LOCK.acquire()
        with open(tmp, "w", encoding="utf-8") as f:
            _acquire_file_lock(f)
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
            _release_file_lock(f)
        os.replace(tmp, path)
    except Exception as e:
        _log("[persist] Falha ao escrever JSON:", e)
    finally:
        try:
            if _MEM_LOCK:
                _MEM_LOCK.release()
        except Exception:
            pass

def _merge_persisted_into_df(df: pd.DataFrame, persisted: dict) -> pd.DataFrame:
    """Aplica valores persistidos no DataFrame por 'nome_do_veiculo'."""
    df = df.copy()
    def _get(nome, key):
        try:
            return float(persisted.get(str(nome), {}).get(key, 0) or 0)
        except Exception:
            return 0.0
    df["valor_planejado"] = df["nome_do_veiculo"].apply(lambda n: _get(n, "valor_planejado"))
    df["valor_pago"]      = df["nome_do_veiculo"].apply(lambda n: _get(n, "valor_pago"))
    df["saldo"] = df["valor_planejado"] - df["valor_pago"]
    return df

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

def _find_first(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _find_by_tokens(df: pd.DataFrame, tokens: list[str]) -> Optional[str]:
    for c in df.columns:
        if all(tok in c for tok in tokens):
            return c
    return None

def _resolve_views(df: pd.DataFrame, mes: str) -> Optional[str]:
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

# ========= CARREGAMENTO DE DADOS (ROBUSTO) =========
def _build_gsheets_csv_url() -> Optional[str]:
    if SHEETS_GSHEET_ID and SHEETS_GID:
        return f"https://docs.google.com/spreadsheets/d/{SHEETS_GSHEET_ID}/export?format=csv&gid={SHEETS_GID}"
    return None

def _read_csv_resilient(url: str) -> Optional[pd.DataFrame]:
    """
    Tenta ler CSV remoto com diferentes estratégias:
    - Pandas read_csv com encoding/engine variados
    - urllib.request + BytesIO se necessário
    Retorna None se falhar em todas.
    """
    if not url:
        return None

    candidates = []
    # prioridade: pandas direto
    candidates.append(("pd_csv_utf8", dict(filepath_or_buffer=_url_with_cache_bust(url),
                                           encoding="utf-8", engine="python", on_bad_lines="skip")))
    candidates.append(("pd_csv_utf8sig", dict(filepath_or_buffer=_url_with_cache_bust(url),
                                              encoding="utf-8-sig", engine="python", on_bad_lines="skip")))
    candidates.append(("pd_csv_latin1", dict(filepath_or_buffer=_url_with_cache_bust(url),
                                             encoding="latin1", engine="python", on_bad_lines="skip")))
    # urllib + pandas
    def _fetch_bytes(u: str) -> Optional[bytes]:
        try:
            import urllib.request
            req = urllib.request.Request(
                _url_with_cache_bust(u),
                headers={"User-Agent": "Mozilla/5.0 (DashBot/1.0) Python"}
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read()
        except Exception as e:
            _log("[data] urllib falhou:", e)
            return None

    raw = _fetch_bytes(url)
    if raw:
        candidates.append(("bytes_utf8", dict(filepath_or_buffer=BytesIO(raw),
                                              encoding="utf-8", engine="python", on_bad_lines="skip")))
        candidates.append(("bytes_utf8sig", dict(filepath_or_buffer=BytesIO(raw),
                                                 encoding="utf-8-sig", engine="python", on_bad_lines="skip")))
        candidates.append(("bytes_latin1", dict(filepath_or_buffer=BytesIO(raw),
                                                encoding="latin1", engine="python", on_bad_lines="skip")))

    for tag, kwargs in candidates:
        try:
            df = pd.read_csv(**kwargs)
            # limpeza básica
            if df is not None and isinstance(df, pd.DataFrame):
                # remove colunas "Unnamed" frequentes no Sheets
                df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
                if df.shape[1] > 0:
                    _log(f"[data] OK via {tag}: linhas={len(df)}, colunas={list(df.columns)[:6]}{'...' if df.shape[1]>6 else ''}")
                    return df
        except Exception as e:
            _log(f"[data] tentativa {tag} falhou:", e)

    return None

def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    original_cols = list(df.columns)
    df.columns = [_normalize(c) for c in df.columns]
    _log("[prepare] Colunas normalizadas:", df.columns.tolist())

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
            _log(f"[prepare] NÃO encontrei coluna de visualizações de {mes.upper()} nas colunas originais:", original_cols)
            df[f"visualizacoes_{mes}"] = 0.0

    df["total_visualizacoes"] = (
        df.get("visualizacoes_julho", 0.0) + df.get("visualizacoes_agosto", 0.0) + df.get("visualizacoes_setembro", 0.0)
    )
    df["media_trimestral"] = df[["visualizacoes_julho", "visualizacoes_agosto", "visualizacoes_setembro"]].mean(axis=1)

    return df

def _empty_df_skeleton() -> pd.DataFrame:
    cols = [
        "nome_fantasia","nome_do_veiculo","cidade","status","motivo","categoria",
        "visualizacoes_julho","visualizacoes_agosto","visualizacoes_setembro",
        "total_visualizacoes","media_trimestral","url"
    ]
    return pd.DataFrame(columns=cols)

def load_data() -> pd.DataFrame:
    """
    Ordem de carregamento:
    1) Se FORCE_LOCAL_DATA=1: tenta Excel local; se falha, devolve vazio preparado
    2) Tenta SHEETS_CSV_URL (se existir) com leitor resiliente
    3) Tenta construir URL via SHEETS_GSHEET_ID + SHEETS_GID
    4) Tenta Excel local
    5) Fallback vazio preparado
    """
    # 1) Force local
    if FORCE_LOCAL_DATA:
        _log("[data] FORCE_LOCAL_DATA=1 → tentar Excel local")
        try:
            base = pd.read_excel(EXCEL_PATH)
            _log("[data] Excel local OK. Linhas:", len(base))
            return _prepare_df(base)
        except Exception as e:
            _log("[data] Excel local falhou (FORCE_LOCAL). Volta vazio:", e)
            return _prepare_df(_empty_df_skeleton())

    # 2) SHEETS_CSV_URL direto
    if SHEETS_CSV_URL:
        _log("[data] Tentando SHEETS_CSV_URL…")
        try:
            raw = _read_csv_resilient(SHEETS_CSV_URL)
            if raw is not None and raw.shape[0] >= 0 and raw.shape[1] >= 1:
                _log("[data] Sheets OK via URL. Linhas:", len(raw))
                return _prepare_df(raw)
            else:
                _log("[data] Sheets URL sem colunas/lb. Ignorando.")
        except Exception as e:
            _log("[data] Falha lendo SHEETS_CSV_URL:", e)

    # 3) Construir URL do Sheets por ID+GID
    built_url = _build_gsheets_csv_url()
    if built_url:
        _log("[data] Tentando URL construída do Google Sheets…")
        try:
            raw2 = _read_csv_resilient(built_url)
            if raw2 is not None and raw2.shape[1] >= 1:
                _log("[data] Sheets OK via ID+GID. Linhas:", len(raw2))
                return _prepare_df(raw2)
            else:
                _log("[data] URL ID+GID sem colunas úteis.")
        except Exception as e:
            _log("[data] Falha lendo URL construída:", e)

    # 4) Excel local
    try:
        base = pd.read_excel(EXCEL_PATH)
        _log("[data] Excel local OK. Linhas:", len(base))
        return _prepare_df(base)
    except Exception as e:
        _log("[data] Excel local indisponível:", e)

    # 5) Fallback vazio
    _log("[data] Todas as fontes falharam. Retornando DF vazio preparado.")
    return _prepare_df(_empty_df_skeleton())

# Base e valores persistidos na inicialização
DF_BASE = load_data()
PERSISTED_VALUES = _read_persisted_vals()

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
    # Armazenamento no navegador + inicialização com JSON persistido do servidor
    dcc.Store(id="store_valores", storage_type="session", data=PERSISTED_VALUES),
    html.Div(className="container", children=[
        # Navbar
        html.Div(className="navbar", children=[
            html.Div(className="brand", children=[
                html.Div("📊", style={"fontSize": "20px"}),
                html.H1("Métricas de Veículos"),
                html.Span("v3.8.2", className="badge"),
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

        # Tabela contínua
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
                        {"if": {"column_id": "nome_do_veiculo"}, "minWidth": "260px", "width": "320px", "maxWidth": "520px"},
                        {"if": {"column_id": "cidade"}, "minWidth": "120px", "width": "140px", "maxWidth": "200px"},
                        {"if": {"column_id": "status"}, "minWidth": "120px", "width": "140px", "maxWidth": "200px"},
                        {"if": {"column_id": "motivo"}, "minWidth": "240px", "width": "360px", "maxWidth": "560px"},
                        {"if": {"column_id": "media_trimestral"}, "textAlign": "right"},
                        {"if": {"column_id": "valor_planejado"}, "textAlign": "right"},
                        {"if": {"column_id": "valor_pago"}, "textAlign": "right"},
                        {"if": {"column_id": "saldo"}, "textAlign": "right"},
                    ],
                ),
            ]),
        ]),
    ]),
])

# ========= FILTRO =========
def _filtrar(base: pd.DataFrame, cidade, status, categoria, sites) -> pd.DataFrame:
    dff = base.copy()
    if cidade:    dff = dff[dff["cidade"].isin(cidade)]
    if status:    dff = dff[dff["status"].isin(status)]
    if categoria: dff = dff[dff["categoria"].isin(categoria)]
    if sites:     dff = dff[dff["nome_do_veiculo"].astype(str).isin(sites)]
    return dff

@app.callback(Output("root", "className"), Input("theme-toggle", "value"))
def set_theme(theme): return "light" if theme == "light" else "dark"

# ========= KPI, GRÁFICOS e TABELA =========
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
    Input("f_sites", "value"),
    Input("f_valor_nzero", "value"),
    Input("sort-order", "value"),
    Input("btn-reload", "n_clicks"),
    State("theme-toggle", "value"),
    State("store_valores", "data"),
)
def atualizar(f_cidade, f_status, f_categoria, f_sites, f_valor_nzero, order, n_reload, theme, store_vals):
    # Sempre tentar recarregar dados quando o usuário clicar "Atualizar dados"
    base = load_data() if (n_reload and n_reload > 0) else DF_BASE

    # lê do arquivo e mescla com o que estiver no store do cliente
    persisted = _read_persisted_vals()
    store_vals = store_vals or persisted or {}
    merged_vals = {**persisted, **(store_vals or {})}

    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_sites)
    dff = _merge_persisted_into_df(dff, merged_vals)

    ascending = (order == "asc")

    # filtro: Somente Valor Pago ≠ 0
    if f_valor_nzero and "nz" in f_valor_nzero:
        dff = dff[(dff["valor_pago"].abs() > 0)]

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
                "APROVADO": "#22C55E","REPROVADO": "#EF4444",
                "APROVADO PARCIAL": "#F59E0B","PENDENTE":"#A78BFA","INSTA":"#06B6D4",
            },
        )
        fig_status.update_traces(textposition="outside")
        fig_status.update_layout(showlegend=True,
                                 xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()))
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
        fig_cidades.update_layout(showlegend=False,
                                  xaxis=dict(categoryorder="array", categoryarray=base_cid["cidade"].tolist()))
    else:
        fig_cidades = px.bar(title="Top 10 Cidades")
    style_fig(fig_cidades, theme)

    # Visualizações por mês (Jul/Ago/Set)
    vjul = float(dff.get("visualizacoes_julho", pd.Series(dtype=float)).sum())
    vago = float(dff.get("visualizacoes_agosto", pd.Series(dtype=float)).sum())
    vset = float(dff.get("visualizacoes_setembro", pd.Series(dtype=float)).sum())
    g3 = pd.DataFrame({"Mês": ["Julho", "Agosto", "Setembro"], "Visualizações": [vjul, vago, vset]}).sort_values(
        "Visualizações", ascending=ascending
    )
    seq3 = get_sequence(theme, len(g3))
    fig_meses = px.bar(
        g3, x="Mês", y="Visualizações", text="Visualizações",
        title="Total de Visualizações por Mês (Jul/Ago/Set)",
        color="Mês", color_discrete_sequence=seq3,
    )
    fig_meses.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig_meses.update_layout(showlegend=False,
                            xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()))
    style_fig(fig_meses, theme)

    # Top sites — média_trimestral (Jul/Ago/Set)
    if {"nome_fantasia","media_trimestral"}.issubset(dff.columns) and not dff.empty:
        g4 = dff.nlargest(10, "media_trimestral")[["nome_fantasia","media_trimestral"]]
        g4 = g4.sort_values("media_trimestral", ascending=ascending)
        seq4 = get_sequence(theme, len(g4))
        fig_sites = px.bar(
            g4, x="media_trimestral", y="nome_fantasia", orientation="h",
            text="media_trimestral",
            title="Top 10 Sites (Média Trimestral Jul/Ago/Set)",
            color="nome_fantasia", color_discrete_sequence=seq4,
        )
        fig_sites.update_traces(texttemplate="%{text:.0f}",
                                hovertemplate="%{y}<br>Média trimestral: %{x:.0f}<extra></extra>")
        fig_sites.update_layout(showlegend=False,
                                yaxis=dict(categoryorder="array", categoryarray=g4["nome_fantasia"].tolist()),
                                xaxis_title="Média Trimestral", yaxis_title="Site")
    else:
        fig_sites = px.bar(title="Top 10 Sites (Média Trimestral Jul/Ago/Set)")
    style_fig(fig_sites, theme)

    # Tabela
    cols_order = ["nome_do_veiculo","cidade","status","motivo",
                  "media_trimestral","valor_planejado","valor_pago","saldo"]
    friendly = {
        "nome_do_veiculo":"Nome do Veículo","cidade":"Cidade","status":"Status","motivo":"Motivo",
        "media_trimestral":"Média Trimestral","valor_planejado":"Valor Planejado",
        "valor_pago":"Valor Pago","saldo":"Saldo"
    }
    present = [c for c in cols_order if c in dff.columns]
    fmt_int0 = Format(group=Group.yes, groups=3, group_delimiter=".", decimal_delimiter=",", precision=0, scheme=Scheme.fixed)
    fmt_money = Format(group=Group.yes, groups=3, group_delimiter=".", decimal_delimiter=",", precision=2, scheme=Scheme.fixed)
    columns = []
    for c in present:
        col_def = {"name": friendly.get(c, c), "id": c}
        if c == "media_trimestral":
            col_def.update({"type":"numeric","format":fmt_int0})
        if c in ["valor_planejado", "valor_pago", "saldo"]:
            col_def.update({"type":"numeric","format":fmt_money})
        if c in ["valor_planejado", "valor_pago"]:
            col_def.update({"editable": True})
        columns.append(col_def)
    data = dff[present].to_dict("records")

    return (f"{total}", f"{aprov}", f"{reprov}", f"{cidades_qtd}",
            fig_status, fig_cidades, fig_meses, fig_sites, data, columns)

# ========= PERSISTIR EDIÇÕES =========
@app.callback(
    Output("store_valores", "data"),
    Input("tbl", "data_timestamp"),
    State("tbl", "data"),
    State("store_valores", "data"),
    prevent_initial_call=True
)
def persistir_valores(_, table_data, store_vals):
    current_file = _read_persisted_vals()
    store_vals = store_vals or current_file or {}
    if not table_data:
        return store_vals

    updated = {**current_file, **store_vals}

    for row in table_data:
        nome = str(row.get("nome_do_veiculo", "")).strip()
        if not nome:
            continue
        d = updated.get(nome, {})
        try:
            d["valor_planejado"] = float(row.get("valor_planejado", d.get("valor_planejado", 0)) or 0)
        except Exception:
            d["valor_planejado"] = float(d.get("valor_planejado", 0) or 0)
        try:
            d["valor_pago"] = float(row.get("valor_pago", d.get("valor_pago", 0)) or 0)
        except Exception:
            d["valor_pago"] = float(d.get("valor_pago", 0) or 0)
        updated[nome] = d

    _write_persisted_vals(updated)
    return updated

# ========= REFRESH OPCÕES DE FILTRO =========
@app.callback(
    Output("f_cidade", "options"),
    Output("f_status", "options"),
    Output("f_categoria", "options"),
    Output("f_sites", "options"),
    Input("btn-reload", "n_clicks"),
    prevent_initial_call=True
)
def refresh_filter_options(n):
    d = load_data()
    cidades = [{"label": c, "value": c} for c in sorted(d["cidade"].dropna().unique())] if "cidade" in d else []
    status  = [{"label": s, "value": s} for s in sorted(d["status"].dropna().unique())] if "status" in d else []
    cats    = [{"label": c, "value": c} for c in sorted(d["categoria"].dropna().unique())] if "categoria" in d else []
    sites   = [{"label": n, "value": n} for n in sorted(d["nome_do_veiculo"].dropna().unique())] if "nome_do_veiculo" in d else []
    return cidades, status, cats, sites

# ========= EXPORTS =========
def _prepare_export_df(f_cidade, f_status, f_categoria, f_sites, nz_flag) -> pd.DataFrame:
    base = load_data()
    persisted = _read_persisted_vals()
    dff = _filtrar(base, f_cidade, f_status, f_categoria, f_sites)
    dff = _merge_persisted_into_df(dff, persisted)
    if nz_flag:
        dff = dff[(dff["valor_pago"].abs() > 0)]
    cols_export = ["nome_do_veiculo","cidade","status","motivo",
                   "media_trimestral","valor_planejado","valor_pago","saldo"]
    return dff[[c for c in cols_export if c in dff.columns]].copy()

@app.callback(
    Output("download_excel", "data"),
    Input("btn-export-excel", "n_clicks"),
    State("f_cidade", "value"),
    State("f_status", "value"),
    State("f_categoria", "value"),
    State("f_sites", "value"),
    State("f_valor_nzero", "value"),
    prevent_initial_call=True
)
def exportar_excel(n, f_cidade, f_status, f_categoria, f_sites, f_valor_nzero):
    df = _prepare_export_df(f_cidade, f_status, f_categoria, f_sites, nz_flag=("nz" in (f_valor_nzero or [])))
    try:
        return dcc.send_data_frame(df.to_excel, "metricas_de_veiculos.xlsx", sheet_name="Dados", index=False)
    except Exception as e:
        _log("[export_excel] Falhou to_excel, fallback para CSV:", e)
        return dcc.send_data_frame(df.to_csv, "metricas_de_veiculos.csv", index=False)

@app.callback(
    Output("download_pdf", "data"),
    Input("btn-export-pdf", "n_clicks"),
    State("f_cidade", "value"),
    State("f_status", "value"),
    State("f_categoria", "value"),
    State("f_sites", "value"),
    State("f_valor_nzero", "value"),
    State("sort-order", "value"),
    State("theme-toggle", "value"),
    prevent_initial_call=True
)
def exportar_pdf(n, f_cidade, f_status, f_categoria, f_sites, f_valor_nzero, order, theme):
    df_base = _prepare_export_df(f_cidade, f_status, f_categoria, f_sites, nz_flag=("nz" in (f_valor_nzero or [])))
    ascending = (order == "asc")
    pdf_theme = "light"

    # Status
    if "status" in df_base and not df_base.empty:
        g1 = df_base["status"].astype(str).str.upper().value_counts().reset_index()
        g1.columns = ["status", "qtd"]
        g1 = g1.sort_values("qtd", ascending=ascending)
        fig_status = px.bar(g1, x="status", y="qtd", text="qtd", title="Distribuição por Status", color="status",
                            color_discrete_map={"APROVADO":"#22C55E","REPROVADO":"#EF4444","APROVADO PARCIAL":"#F59E0B","PENDENTE":"#A78BFA","INSTA":"#06B6D4"})
        fig_status.update_traces(textposition="outside")
        fig_status.update_layout(showlegend=True, xaxis=dict(categoryorder="array", categoryarray=g1["status"].tolist()))
    else:
        fig_status = px.bar(title="Distribuição por Status")
    style_fig(fig_status, pdf_theme); fig_status.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    # Top Cidades
    if "cidade" in df_base and not df_base.empty:
        base_cid = df_base["cidade"].value_counts().reset_index()
        base_cid.columns = ["cidade","qtd"]
        base_cid = base_cid.sort_values("qtd", ascending=False).head(10)
        base_cid = base_cid.sort_values("qtd", ascending=ascending)
        seq = get_sequence(pdf_theme, len(base_cid))
        fig_cidades = px.bar(base_cid, x="cidade", y="qtd", text="qtd", title="Top 10 Cidades", color="cidade", color_discrete_sequence=seq)
        fig_cidades.update_traces(textposition="outside")
        fig_cidades.update_layout(showlegend=False, xaxis=dict(categoryorder="array", categoryarray=base_cid["cidade"].tolist()))
    else:
        fig_cidades = px.bar(title="Top 10 Cidades")
    style_fig(fig_cidades, pdf_theme); fig_cidades.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    # Meses (Jul/Ago/Set) — com base nos dados brutos
    base_raw = _filtrar(load_data(), f_cidade, f_status, f_categoria, f_sites)
    base_raw = _merge_persisted_into_df(base_raw, _read_persisted_vals())
    vjul = float(base_raw.get("visualizacoes_julho", pd.Series(dtype=float)).sum())
    vago = float(base_raw.get("visualizacoes_agosto", pd.Series(dtype=float)).sum())
    vset = float(base_raw.get("visualizacoes_setembro", pd.Series(dtype=float)).sum())
    g3 = pd.DataFrame({"Mês":["Julho","Agosto","Setembro"], "Visualizações":[vjul, vago, vset]}).sort_values("Visualizações", ascending=ascending)
    seq3 = get_sequence(pdf_theme, len(g3))
    fig_meses = px.bar(g3, x="Mês", y="Visualizações", text="Visualizações", title="Total de Visualizações por Mês (Jul/Ago/Set)",
                       color="Mês", color_discrete_sequence=seq3)
    fig_meses.update_traces(texttemplate="%{text:.0f}", textposition="outside")
    fig_meses.update_layout(showlegend=False, xaxis=dict(categoryorder="array", categoryarray=g3["Mês"].tolist()))
    style_fig(fig_meses, pdf_theme); fig_meses.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    # Top Sites por Média (Jul/Ago/Set)
    if {"nome_do_veiculo","media_trimestral"}.issubset(base_raw.columns) and not base_raw.empty:
        g4 = base_raw.nlargest(10, "media_trimestral")[["nome_fantasia","media_trimestral"]]
        g4 = g4.sort_values("media_trimestral", ascending=ascending)
        seq4 = get_sequence(pdf_theme, len(g4))
        fig_sites = px.bar(g4, x="media_trimestral", y="nome_fantasia", orientation="h", text="media_trimestral",
                           title="Top 10 Sites (Média Trimestral Jul/Ago/Set)", color="nome_fantasia", color_discrete_sequence=seq4)
        fig_sites.update_traces(texttemplate="%{text:.0f}")
        fig_sites.update_layout(showlegend=False, yaxis=dict(categoryorder="array", categoryarray=g4["nome_fantasia"].tolist()),
                                xaxis_title="Média Trimestral", yaxis_title="Site")
    else:
        fig_sites = px.bar(title="Top 10 Sites (Média Trimestral Jul/Ago/Set)")
    style_fig(fig_sites, pdf_theme); fig_sites.update_layout(paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF")

    figs = [fig_status, fig_cidades, fig_meses, fig_sites]

    def _to_pdf(bytes_io):
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.units import mm

        page_size = landscape(A4)
        left_margin = right_margin = top_margin = bottom_margin = 12 * mm
        avail_w = page_size[0] - left_margin - right_margin

        doc = SimpleDocTemplate(bytes_io, pagesize=page_size,
                                leftMargin=left_margin, rightMargin=right_margin,
                                topMargin=top_margin, bottomMargin=bottom_margin)

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle("title", parent=styles["Heading2"], alignment=0, fontSize=14, leading=16, textColor=colors.HexColor("#111827"))
        header_style = ParagraphStyle("header", parent=styles["Normal"], fontSize=9, textColor=colors.white)
        cell_text = ParagraphStyle("cell", parent=styles["Normal"], fontSize=8, leading=10)
        cell_wrap = ParagraphStyle("cell_wrap", parent=cell_text, wordWrap="CJK")

        story = [Paragraph("Métricas de Veículos — Relatório", title_style), Spacer(1, 6)]

        def fig_to_rlimage(fig, width_pt):
            try:
                img_bytes = pio.to_image(fig, format="png", scale=2)  # requer kaleido
                height_pt = width_pt * 9.0 / 16.0
                return RLImage(BytesIO(img_bytes), width=width_pt, height=height_pt)
            except Exception as e:
                _log("[export_pdf] Falha ao renderizar gráfico com kaleido:", e)
                return None

        col_w = (avail_w - 6*mm) / 2.0
        row_imgs = []
        for i, fig in enumerate(figs):
            rlimg = fig_to_rlimage(fig, col_w)
            row_imgs.append(rlimg or Paragraph("**Gráfico indisponível (kaleido ausente)**", cell_text))
            if (i % 2 == 1) or (i == len(figs)-1):
                t = Table([[row_imgs[0]] + ([row_imgs[1]] if len(row_imgs) > 1 else [])],
                          colWidths=[col_w, col_w] if len(row_imgs) > 1 else [col_w])
                t.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),
                                       ("LEFTPADDING",(0,0),(-1,-1),0),
                                       ("RIGHTPADDING",(0,0),(-1,-1),0),
                                       ("TOPPADDING",(0,0),(-1,-1),0),
                                       ("BOTTOMPADDING",(0,0),(-1,-1),0)]))
                story += [t, Spacer(1, 8)]
                row_imgs = []

        labels = {
            "nome_do_veiculo":"Nome do Veículo","cidade":"Cidade","status":"Status",
            "motivo":"Motivo","media_trimestral":"Média Trimestral",
            "valor_planejado":"Valor Planejado","valor_pago":"Valor Pago","saldo":"Saldo"
        }
        col_keys = [c for c in ["nome_do_veiculo","cidade","status","motivo",
                                "media_trimestral","valor_planejado","valor_pago","saldo"] if c in df_base.columns]
        headers = [labels[k] for k in col_keys]
        data = [[Paragraph(h, header_style) for h in headers]]

        def fmt_int(x):
            try: return str(f"{int(round(float(x))):,}").replace(",", ".")
            except: return str(x)
        def fmt_money(x):
            try:
                s = f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                return s
            except:
                return str(x)

        for _, row in df_base[col_keys].iterrows():
            line = []
            for k in col_keys:
                v = row[k]
                if k == "media_trimestral":
                    line.append(Paragraph(fmt_int(v), cell_text))
                elif k in ["valor_planejado", "valor_pago", "saldo"]:
                    line.append(Paragraph(fmt_money(v), cell_text))
                elif k in ["nome_do_veiculo","motivo"]:
                    line.append(Paragraph(str(v), cell_wrap))
                else:
                    line.append(Paragraph(str(v), cell_text))
            data.append(line)

        weights = {"nome_do_veiculo":3.2,"cidade":1.2,"status":1.1,"motivo":3.2,
                   "media_trimestral":1.4,"valor_planejado":1.5,"valor_pago":1.5,"saldo":1.5}
        wlist = [weights.get(k,1.0) for k in col_keys]
        col_widths = [(w/sum(wlist))*avail_w for w in wlist]

        from reportlab.platypus import Table, TableStyle
        from reportlab.lib import colors
        tbl = Table(data, colWidths=col_widths, repeatRows=1); tbl.splitByRow = 1
        styles_tbl = [
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#111827")),
            ("TEXTCOLOR",(0,0),(-1,0),colors.white),
            ("ALIGN",(0,0),(-1,0),"CENTER"),
            ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#D1D5DB")),
            ("VALIGN",(0,0),(-1,-1),"TOP"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.whitesmoke, colors.HexColor("#F8FAFC")]),
            ("LEFTPADDING",(0,0),(-1,-1),4),("RIGHTPADDING",(0,0),(-1,-1),4),
            ("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),
        ]
        for idx, k in enumerate(col_keys):
            if k in ["media_trimestral","valor_planejado","valor_pago","saldo"]:
                styles_tbl.append(("ALIGN",(idx,1),(idx,-1),"RIGHT"))
        tbl.setStyle(TableStyle(styles_tbl))
        story += [Spacer(1,4), tbl]
        doc.build(story)

    return dcc.send_bytes(_to_pdf, "metricas_de_veiculos.pdf")

# ========= REFRESH COMPLETO (dados + valores) =========
@app.callback(
    Output("store_valores", "data"),
    Input("btn-reload", "n_clicks"),
    State("store_valores", "data"),
    prevent_initial_call=True
)
def recarregar_store(n, current_store):
    # re-sincroniza o store com o arquivo no disco (mantendo qualquer edição que já esteja no store)
    persisted = _read_persisted_vals()
    merged = {**(persisted or {}), **(current_store or {})}
    _write_persisted_vals(merged)
    return merged

# RUN
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    try:
        app.run(debug=True, host="0.0.0.0", port=port)
    except AttributeError:
        app.run_server(debug=True, host="0.0.0.0", port=port)
