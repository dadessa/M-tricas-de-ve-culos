#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.main import load_excel_data, load_google_sheets_data

print('Testando carregamento Excel...')
try:
    data_excel = load_excel_data()
    print(f'Excel: {len(data_excel)} registros')
    if data_excel:
        print('Primeira linha Excel:', list(data_excel[0].keys())[:5])
        print('Valores primeira linha:', list(data_excel[0].values())[:5])
except Exception as e:
    print(f'Erro Excel: {e}')

print('\nTestando carregamento Google Sheets...')
try:
    data_sheets = load_google_sheets_data()
    print(f'Google Sheets: {len(data_sheets)} registros')
    if data_sheets:
        print('Primeira linha Sheets:', list(data_sheets[0].keys())[:5])
except Exception as e:
    print(f'Erro Google Sheets: {e}')

print('\nTestando inicialização global...')
try:
    from src.main import data
    print(f'Dados globais: {len(data)} registros')
    if data:
        print('Primeira linha global:', list(data[0].keys())[:5])
except Exception as e:
    print(f'Erro dados globais: {e}')

