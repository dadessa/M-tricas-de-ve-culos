import os
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output

# ===========================
# Configuração do caminho do Excel
# ===========================
# Use a variável de ambiente EXCEL_PATH, se existir; caso contrário, tente arquivo local.
DEFAULT_FILE = "Recadastramento(respostas)(2).xlsx"
EXCEL_PATH = os.environ.get("EXCEL_PATH", DEFAULT_FILE)

# ===========================
# Carregamento dos dados com tratamento de erro
# ===========================
def load_data(path):
    if not os.path.exists(path):
        # Se o arquivo não existir, exiba uma interface de aviso
        return None
    try:
        df = pd.read_excel(path)
        # Renomear colunas para facilitar o uso
        df.columns = [
            'timestamp', 'nome_fantasia', 'razao_social', 'cnpj', 'endereco', 
            'telefone_empresa', 'email_comercial', 'responsavel_tecnico', 'url', 
            'relatorio_analytics', 'declaracao_veracidade', 'acesso_analytics', 
            'modalidade_site', 'telefone_responsavel', 'email_responsavel', 
            'nome_social', 'cidade', 'expediente', 'cookies', 'endereco_site', 
            'visualizacoes_junho', 'visualizacoes_julho', 'visualizacoes_agosto', 
            'categoria', 'modalidade', 'google_analytics', 'propriedade', 'status'
        ]
        # Limpeza e preparação dos dados
        def clean_numeric_column(col):
            if col.dtype == 'object':
                return pd.to_numeric(col.astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0)
            return col
        for col in ['visualizacoes_junho','visualizacoes_julho','visualizacoes_agosto']:
            df[col] = clean_numeric_column(df[col])
        df['total_visualizacoes'] = df['visualizacoes_junho'] + df['visualizacoes_julho'] + df['visualizacoes_agosto']
        for col in ['cidade','categoria','status']:
            df[col] = df[col].fillna('Não informado')
        return df
    except Exception as e:
        # Se der erro ao ler, retorne None e o app mostrará mensagem
        print(f"[ERRO] Falha ao ler Excel: {e}")
        return None

df = load_data(EXCEL_PATH)

# ===========================
# App Dash
# ===========================
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server  # Necessário para gunicorn

if df is None:
    # Layout alternativo exibindo instruções se o arquivo não for encontrado/lido
    app.layout = html.Div([
        html.H2("Dashboard de Análise de Sites - Recadastramento"),
        html.H4("Arquivo de dados não encontrado ou inválido."),
        html.P([
            "Defina a variável de ambiente ",
            html.Code("EXCEL_PATH"),
            " com o caminho completo/relativo para o arquivo Excel, ou coloque o arquivo ",
            html.Code(DEFAULT_FILE),
            " na mesma pasta do aplicativo."
        ]),
        html.P("Exemplos:"),
        html.Pre("EXCEL_PATH=/caminho/para/Recadastramento(respostas)(2).xlsx"),
    ], style={'padding':'30px'})
else:
    app.layout = html.Div([
        html.Div([
            html.H1('Dashboard de Análise de Sites - Recadastramento', 
                    style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 30})
        ]),

        # Seção de filtros
        html.Div([
            html.H3('Filtros', style={'color': '#34495e'}),
            html.Div([
                html.Div([
                    html.Label('Cidade:', style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='filtro-cidade',
                        options=[{'label': i, 'value': i} for i in sorted(df['cidade'].unique())],
                        multi=True,
                        placeholder='Selecione as cidades...',
                        style={'marginBottom': 10}
                    )
                ], className='four columns'),

                html.Div([
                    html.Label('Categoria:', style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='filtro-categoria',
                        options=[{'label': i, 'value': i} for i in sorted(df['categoria'].unique())],
                        multi=True,
                        placeholder='Selecione as categorias...',
                        style={'marginBottom': 10}
                    )
                ], className='four columns'),

                html.Div([
                    html.Label('Status:', style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='filtro-status',
                        options=[{'label': i, 'value': i} for i in sorted(df['status'].unique())],
                        multi=True,
                        placeholder='Selecione os status...',
                        style={'marginBottom': 10}
                    )
                ], className='four columns')
            ], className='row', style={'marginBottom': 20})
        ], style={'backgroundColor': '#ecf0f1', 'padding': 20, 'marginBottom': 20}),

        # Seção de métricas principais
        html.Div(id='metricas-principais', style={'marginBottom': 20}),

        # Seção de gráficos
        html.Div([
            html.Div([dcc.Graph(id='grafico-visualizacoes-mes')], className='six columns'),
            html.Div([dcc.Graph(id='grafico-sites-por-cidade')], className='six columns')
        ], className='row'),

        html.Div([
            html.Div([dcc.Graph(id='grafico-categoria-status')], className='six columns'),
            html.Div([dcc.Graph(id='grafico-top-sites')], className='six columns')
        ], className='row'),

        # Tabela de dados
        html.Div([
            html.H3('Dados Detalhados', style={'color': '#34495e'}),
            html.Div(id='tabela-dados')
        ], style={'marginTop': 30})
    ])

    @app.callback(
        [Output('metricas-principais', 'children'),
         Output('grafico-visualizacoes-mes', 'figure'),
         Output('grafico-sites-por-cidade', 'figure'),
         Output('grafico-categoria-status', 'figure'),
         Output('grafico-top-sites', 'figure'),
         Output('tabela-dados', 'children')],
        [Input('filtro-cidade', 'value'),
         Input('filtro-categoria', 'value'),
         Input('filtro-status', 'value')]
    )
    def update_dashboard(cidades_selecionadas, categorias_selecionadas, status_selecionado):
        dff = df.copy()
        if cidades_selecionadas:
            dff = dff[dff['cidade'].isin(cidades_selecionadas)]
        if categorias_selecionadas:
            dff = dff[dff['categoria'].isin(categorias_selecionadas)]
        if status_selecionado:
            dff = dff[dff['status'].isin(status_selecionado)]

        total_sites = len(dff)
        total_vis_junho = dff['visualizacoes_junho'].sum()
        total_vis_julho = dff['visualizacoes_julho'].sum()
        total_vis_agosto = dff['visualizacoes_agosto'].sum()
        media_vis_site = dff['total_visualizacoes'].mean() if total_sites > 0 else 0

        metricas = html.Div([
            html.Div([
                html.H4(f'{total_sites:,}', style={'color': '#3498db', 'fontSize': 36, 'margin': 0}),
                html.P('Total de Sites', style={'margin': 0})
            ], className='three columns', style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': 20, 'borderRadius': 5}),

            html.Div([
                html.H4(f'{total_vis_junho:,.0f}', style={'color': '#e74c3c', 'fontSize': 36, 'margin': 0}),
                html.P('Visualizações Junho', style={'margin': 0})
            ], className='three columns', style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': 20, 'borderRadius': 5}),

            html.Div([
                html.H4(f'{total_vis_julho:,.0f}', style={'color': '#f39c12', 'fontSize': 36, 'margin': 0}),
                html.P('Visualizações Julho', style={'margin': 0})
            ], className='three columns', style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': 20, 'borderRadius': 5}),

            html.Div([
                html.H4(f'{total_vis_agosto:,.0f}', style={'color': '#27ae60', 'fontSize': 36, 'margin': 0}),
                html.P('Visualizações Agosto', style={'margin': 0})
            ], className='three columns', style={'textAlign': 'center', 'backgroundColor': 'white', 'padding': 20, 'borderRadius': 5})
        ], className='row')

        import plotly.express as px
        meses = ['Junho', 'Julho', 'Agosto']
        visualizacoes = [total_vis_junho, total_vis_julho, total_vis_agosto]
        fig_vis = px.bar(x=meses, y=visualizacoes, title='Total de Visualizações por Mês',
                        color=visualizacoes, color_continuous_scale='viridis')
        fig_vis.update_layout(showlegend=False, xaxis_title='Mês', yaxis_title='Visualizações')

        df_cidade = dff['cidade'].value_counts().head(10)
        fig_cidade = px.pie(values=df_cidade.values, names=df_cidade.index, 
                        title='Top 10 Cidades por Número de Sites')

        df_cat_status = dff.groupby(['categoria', 'status']).size().reset_index(name='count')
        fig_cat_status = px.sunburst(df_cat_status, path=['categoria', 'status'], values='count',
                                    title='Distribuição por Categoria e Status')

        top_sites = dff.nlargest(10, 'total_visualizacoes')[['nome_fantasia', 'total_visualizacoes']]
        fig_top = px.bar(top_sites, x='total_visualizacoes', y='nome_fantasia', 
                        orientation='h', title='Top 10 Sites por Total de Visualizações')
        fig_top.update_layout(yaxis={'categoryorder': 'total ascending'})

        colunas_tabela = ['nome_fantasia', 'cidade', 'categoria', 'status', 'url', 
                        'visualizacoes_junho', 'visualizacoes_julho', 'visualizacoes_agosto', 'total_visualizacoes']
        tabela = dash_table.DataTable(
            data=dff[colunas_tabela].head(20).to_dict('records'),
            columns=[{'name': col.replace('_', ' ').title(), 'id': col} for col in colunas_tabela],
            style_cell={'textAlign': 'left', 'padding': '10px'},
            style_header={'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold'},
            style_data={'backgroundColor': '#ecf0f1'},
            page_size=10,
            sort_action='native',
            filter_action='native'
        )
        return metricas, fig_vis, fig_cidade, fig_cat_status, fig_top, tabela

if __name__ == "__main__":
    # Em local, honre a variável PORT se existir; caso contrário, 8050.
    port = int(os.environ.get("PORT", "8050"))
    # debug só em local: use a env var DASH_DEBUG=1 para habilitar
    debug = os.environ.get("DASH_DEBUG", "0") == "1"
    app.run_server(host="0.0.0.0", port=port, debug=debug)
