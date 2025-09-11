import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# Carregar os dados
df = pd.read_excel('/home/ubuntu/upload/Recadastramento(respostas)(2).xlsx')

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

# Inicializar o aplicativo Dash
app = dash.Dash(__name__)

# Layout do dashboard
app.layout = html.Div([
    html.H1('Dashboard de Análise de Sites'),
    
    # Controles de filtro
    html.Div([
        dcc.Dropdown(
            id='filtro-cidade',
            options=[{'label': i, 'value': i} for i in df['cidade'].unique()],
            multi=True,
            placeholder='Filtrar por cidade...'
        ),
        dcc.Dropdown(
            id='filtro-categoria',
            options=[{'label': i, 'value': i} for i in df['categoria'].unique()],
            multi=True,
            placeholder='Filtrar por categoria...'
        ),
        dcc.Dropdown(
            id='filtro-status',
            options=[{'label': i, 'value': i} for i in df['status'].unique()],
            multi=True,
            placeholder='Filtrar por status...'
        )
    ], style={'width': '50%', 'display': 'inline-block'}),
    
    # Gráficos
    dcc.Graph(id='grafico-visualizacoes-mes'),
    dcc.Graph(id='grafico-sites-por-cidade')
])

# Callbacks para atualizar os gráficos
@app.callback(
    [Output('grafico-visualizacoes-mes', 'figure'),
     Output('grafico-sites-por-cidade', 'figure')],
    [Input('filtro-cidade', 'value'),
     Input('filtro-categoria', 'value'),
     Input('filtro-status', 'value')]
)
def update_graphs(cidades_selecionadas, categorias_selecionadas, status_selecionado):
    dff = df.copy()
    
    if cidades_selecionadas:
        dff = dff[dff['cidade'].isin(cidades_selecionadas)]
    if categorias_selecionadas:
        dff = dff[dff['categoria'].isin(categorias_selecionadas)]
    if status_selecionado:
        dff = dff[dff['status'].isin(status_selecionado)]

    # Gráfico de Visualizações por Mês
    df_vis = dff[['visualizacoes_junho', 'visualizacoes_julho', 'visualizacoes_agosto']].sum().reset_index()
    df_vis.columns = ['Mês', 'Total de Visualizações']
    fig_vis = px.bar(df_vis, x='Mês', y='Total de Visualizações', title='Total de Visualizações por Mês')

    # Gráfico de Sites por Cidade
    df_cidade = dff['cidade'].value_counts().reset_index()
    df_cidade.columns = ['Cidade', 'Número de Sites']
    fig_cidade = px.pie(df_cidade, names='Cidade', values='Número de Sites', title='Distribuição de Sites por Cidade')

    return fig_vis, fig_cidade

if __name__ == '__main__':
    app.run(debug=True, host=\'0.0.0.0\')
