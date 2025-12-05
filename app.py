from flask import Flask, jsonify, render_template, request
import requests
import json
import csv
import io
import os
from datetime import datetime

app = Flask(__name__)

# Configurar CORS manualmente
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# URLs do Google Sheets (múltiplas tentativas)
GOOGLE_SHEETS_URLS = [
    "https://docs.google.com/spreadsheets/d/17TnGB6NpsziDec4fPH-d0TCQwk2LN0BAv6yjmIpyZnI/export?format=csv&gid=1225239898",
    "https://docs.google.com/spreadsheets/d/17TnGB6NpsziDec4fPH-d0TCQwk2LN0BAv6yjmIpyZnI/export?format=csv",
    "https://docs.google.com/spreadsheets/d/17TnGB6NpsziDec4fPH-d0TCQwk2LN0BAv6yjmIpyZnI/gviz/tq?tqx=out:csv&gid=1225239898"
]

def clean_numeric_value(value):
    """Limpa e converte valores numéricos"""
    if not value or str(value).lower() in ['não tem analytics', 'n/a', 'nan', '']:
        return 'N/A'
    
    # Converter para string e limpar
    str_value = str(value).strip()
    
    # Remover caracteres não numéricos exceto pontos e vírgulas
    import re
    cleaned = re.sub(r'[^\d.,]', '', str_value)
    
    # Substituir vírgulas por pontos
    cleaned = cleaned.replace(',', '.')
    
    try:
        # Tentar converter para float e depois para int se for número inteiro
        num_value = float(cleaned)
        if num_value.is_integer():
            return int(num_value)
        return num_value
    except:
        return 'N/A'

def load_data_from_local_csv():
    """Carrega dados do arquivo CSV local"""
    try:
        csv_path = os.path.join(os.path.dirname(__file__), 'Recadastramento(respostas)-CADASTROS(2).csv')
        
        if not os.path.exists(csv_path):
            print(f"Arquivo CSV não encontrado: {csv_path}")
            return []
        
        data = []
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row in reader:
                # Mapear colunas para nomes mais limpos
                clean_row = {}
                
                # Nome do veículo
                nome_col = 'Nome do veículo.\n'
                if nome_col in row:
                    clean_row['Nome do veículo'] = row[nome_col] or 'N/A'
                else:
                    clean_row['Nome do veículo'] = 'N/A'
                
                # Outras colunas
                clean_row['Cidade'] = row.get('Cidade', 'N/A')
                clean_row['Status'] = row.get('Status', 'N/A')
                clean_row['Categoria'] = row.get('Categoria', 'N/A')
                clean_row['Cookies'] = row.get('Cookies', 'N/A')
                clean_row['Expediente'] = row.get('Expediente', 'N/A')
                clean_row['Endereço'] = row.get('Endereço no site', 'N/A')
                clean_row['Analytics'] = row.get('Google analytics ', 'N/A')
                
                # Views com limpeza numérica
                clean_row['Views Set'] = clean_numeric_value(row.get('Views Setembro', '0'))
                clean_row['Views Out'] = clean_numeric_value(row.get('Views Outubro', '0'))
                clean_row['Views Nov'] = clean_numeric_value(row.get('Views Novembro', '0'))
                clean_row['Média Trimestral'] = clean_numeric_value(row.get('Média Trimestral', '0'))
                clean_row['Views Ago'] = clean_numeric_value(row.get('Views Novembro', '0'))  # Usando Nov como Ago
                
                # Só adicionar se tiver nome válido
                if clean_row['Nome do veículo'] != 'N/A' and clean_row['Nome do veículo'].strip():
                    data.append(clean_row)
        
        print(f"Dados carregados do CSV local: {len(data)} registros")
        return data
        
    except Exception as e:
        print(f"Erro ao carregar CSV local: {e}")
        return []

def load_data_from_sheets():
    """Carrega dados diretamente do Google Sheets com múltiplas tentativas"""
    
    # Primeiro tentar carregar do Google Sheets
    for url in GOOGLE_SHEETS_URLS:
        try:
            print(f"Tentando carregar dados do Google Sheets: {url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
            
            if response.status_code == 200 and response.text.strip():
                # Verificar se é HTML (erro) ou CSV
                if response.text.strip().startswith('<'):
                    print(f"Resposta HTML recebida (erro de acesso): {url}")
                    continue
                
                # Tentar processar como CSV
                csv_data = response.text
                reader = csv.DictReader(io.StringIO(csv_data))
                
                data = []
                for row in reader:
                    # Mapear colunas para nomes mais limpos
                    clean_row = {}
                    
                    # Nome do veículo (tentar diferentes variações)
                    nome_options = ['Nome do veículo.\n', 'Nome do veículo', 'Nome']
                    nome_value = 'N/A'
                    for nome_col in nome_options:
                        if nome_col in row and row[nome_col]:
                            nome_value = row[nome_col]
                            break
                    clean_row['Nome do veículo'] = nome_value
                    
                    # Outras colunas
                    clean_row['Cidade'] = row.get('Cidade', 'N/A')
                    clean_row['Status'] = row.get('Status', 'N/A')
                    clean_row['Categoria'] = row.get('Categoria', 'N/A')
                    clean_row['Cookies'] = row.get('Cookies', 'N/A')
                    clean_row['Expediente'] = row.get('Expediente', 'N/A')
                    clean_row['Endereço'] = row.get('Endereço no site', row.get('Endereço', 'N/A'))
                    clean_row['Analytics'] = row.get('Google analytics ', row.get('Analytics', 'N/A'))
                    
                    # Views com limpeza numérica
                    clean_row['Views Set'] = clean_numeric_value(row.get('Views Setembro', '0'))
                    clean_row['Views Out'] = clean_numeric_value(row.get('Views Outubro', '0'))
                    clean_row['Views Nov'] = clean_numeric_value(row.get('Views Novembro', '0'))
                    clean_row['Média Trimestral'] = clean_numeric_value(row.get('Média Trimestral', '0'))
                    clean_row['Views Ago'] = clean_numeric_value(row.get('Views Novembro', '0'))  # Usando Nov como Ago
                    
                    # Só adicionar se tiver nome válido
                    if clean_row['Nome do veículo'] != 'N/A' and clean_row['Nome do veículo'].strip():
                        data.append(clean_row)
                
                if len(data) > 0:
                    print(f"Dados carregados do Google Sheets: {len(data)} registros")
                    return data
                else:
                    print(f"Google Sheets retornou dados vazios: {url}")
                    continue
                    
        except Exception as e:
            print(f"Erro ao carregar dados do Google Sheets ({url}): {e}")
            continue
    
    # Se todas as tentativas falharam, usar CSV local
    print("Todas as tentativas do Google Sheets falharam, usando CSV local...")
    return load_data_from_local_csv()

# Cache dos dados
cached_data = []
last_update = None
data_source = "local"

@app.route('/')
def index():
    """Página principal do dashboard"""
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    """API para obter todos os dados"""
    global cached_data, last_update, data_source
    
    # Recarregar dados se não existirem ou se passaram mais de 5 minutos
    current_time = datetime.now()
    if not cached_data or not last_update or (current_time - last_update).seconds > 300:
        cached_data = load_data_from_sheets()
        last_update = current_time
        
        # Determinar fonte dos dados
        if len(cached_data) > 0:
            # Verificar se conseguiu carregar do Google Sheets (mais de 130 registros indica sucesso)
            data_source = "sheets" if len(cached_data) > 130 else "local"
        else:
            data_source = "local"
    
    return jsonify(cached_data)

@app.route('/api/refresh')
def refresh_data():
    """API para forçar atualização dos dados"""
    global cached_data, last_update, data_source
    
    print("Forçando atualização dos dados...")
    cached_data = load_data_from_sheets()
    last_update = datetime.now()
    
    # Determinar fonte dos dados
    if len(cached_data) > 0:
        data_source = "sheets" if len(cached_data) > 130 else "local"
    else:
        data_source = "local"
    
    source_text = "Google Sheets" if data_source == "sheets" else "Arquivo local"
    
    return jsonify({
        'success': True,
        'message': f'Dados atualizados com sucesso do {source_text}. {len(cached_data)} registros carregados.',
        'timestamp': last_update.isoformat(),
        'total_records': len(cached_data),
        'source': source_text
    })

@app.route('/api/stats')
def get_stats():
    """API para obter estatísticas dos dados"""
    global cached_data, data_source
    
    if not cached_data:
        cached_data = load_data_from_sheets()
    
    if not cached_data:
        return jsonify({'error': 'Nenhum dado disponível'})
    
    # Calcular estatísticas manualmente
    total_veiculos = len(cached_data)
    
    # Contar status
    status_counts = {}
    for item in cached_data:
        status = item.get('Status', 'N/A')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Contar categorias
    categoria_counts = {}
    for item in cached_data:
        categoria = item.get('Categoria', 'N/A')
        categoria_counts[categoria] = categoria_counts.get(categoria, 0) + 1
    
    # Contar cidades
    cidades_set = set()
    for item in cached_data:
        cidade = item.get('Cidade', 'N/A')
        if cidade != 'N/A':
            cidades_set.add(cidade)
    
    source_text = "Google Sheets" if data_source == "sheets" else f"Arquivo local ({len(cached_data)} registros)"
    
    stats = {
        'total_veiculos': total_veiculos,
        'total_cidades': len(cidades_set),
        'status_counts': status_counts,
        'categoria_counts': categoria_counts,
        'data_source': source_text
    }
    
    return jsonify(stats)

@app.route('/api/search')
def search_data():
    """API para buscar dados"""
    global cached_data
    
    if not cached_data:
        cached_data = load_data_from_sheets()
    
    query = request.args.get('q', '').lower().strip()
    
    if not query:
        return jsonify(cached_data)
    
    # Filtrar dados baseado na busca
    filtered_data = []
    for item in cached_data:
        nome = str(item.get('Nome do veículo', '')).lower()
        if query in nome:
            filtered_data.append(item)
    
    return jsonify(filtered_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
