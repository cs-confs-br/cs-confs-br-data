#!/usr/bin/env python3
import pandas as pd
import os
from calc_h5 import run_h5_script

def format_h5(year, sources):
    if not sources:
        fonte_str = ""
    else:
        fontes_unicas = list(dict.fromkeys(sources))
        fonte_str = "[" + "+".join(fontes_unicas) + "]"
    return f"({year}) {fonte_str}"

# arquivos
H5_IGNORE_FILE  = '../h5-gs/h5-gs-ignore.csv'
H5_MAIN_FILE    = '../h5-gs/out-h5-gs-2025-09.csv'
CONFS_LIST_FILE = '../data/confs-list.csv'
OUTPUT_FILE     = '../out/website-2025.csv'
ANO_REF = 2025
PACOTE_DADOS  = '2025_09'

print(f"github.com/cs-confs-br/cs-confs-br-data: BEGIN script generate_website.py")
print(f"-------------------------------")
print(f"H5_IGNORE_FILE = {H5_IGNORE_FILE}")
print(f"H5_MAIN_FILE = {H5_MAIN_FILE}")
print(f"CONFS_LIST_FILE = {CONFS_LIST_FILE}")
print(f"OUTPUT_FILE = {OUTPUT_FILE}")
print(f"ANO_REF = {ANO_REF}")
print(f"PACOTE_DADOS = {PACOTE_DADOS}")
print(f"-------------------------------")


# --- 1. Ler ignorados ---
df_ignore = pd.read_csv(H5_IGNORE_FILE)
ignored_names = set(df_ignore['nome'].tolist())

# --- 2. Ler inclusões manuais ---
df_inclusao = pd.read_csv(CONFS_LIST_FILE)
manual_includes = dict()  # nome_evento -> sigla
for _, row in df_inclusao.iterrows():
    manual_includes[row['Conference']] = row['Acronym']

# --- 3. Ler arquivo principal ---
df_main = pd.read_csv(H5_MAIN_FILE)

# manter registros únicos por título (para facilitar busca)
df_main_grouped = df_main.groupby('nome_evento')

# --- 4. Construir lista final ---
final_rows = []
included_names = set()

for _, row in df_main.iterrows():
    nome = row['nome_evento']
    nome_scholar = row['nome_scholar']
    sigla = row['sigla']
    h5 = row['h5']

    print(f"Processing: {sigla} -> {nome} ({nome_scholar}) ...")
    
    # ignorados
    if nome in ignored_names:
        print(f"   -> Ignorado (h5-gs-ignore.csv): {nome}")
        continue
    else:
        #print("   => nao ignorado!")
        pass

    # duplicatas
    duplicates = df_main_grouped.get_group(nome) if nome in df_main_grouped.groups else pd.DataFrame([row])
    if len(duplicates) > 1:
        print(f"   => WARNING! Duplicate: {sigla} -> {nome} (scholar: {nome_scholar}) with {len(duplicates)} entries...")
        # verifica se algum está na lista de inclusão PELO NOME DO SCHOLAR
        included = []
        for _, dup in duplicates.iterrows():
            if nome_scholar in manual_includes:
                included.append(dup)
                # print(f"DUP = {dup}")
        if included or nome == nome_scholar:
            print(f"   => OK! Incluindo como {nome_scholar} h5 = {h5}")
            for dup in included:
                final_rows.append({
                    'Conference' : nome_scholar,
                    'Acronym' :  dup['sigla'],  # TODO(igormcoelho): buscar sigla correta do arquivo de inclusões...
                    'Year' : 2025,
                    'Topic' : 'General',
                    'Papers(5Y)' : '',
                    'Citations(5Y)' : '',
                    'h5' : int(h5),
                    'h5_source' : format_h5(2025, ['GS'])
                })
                included_names.add(nome_scholar)
            continue
        else:
            print(f"   => WARNING: IGNORED (duplicate without manual inclusion): {nome_scholar}")
            continue

    # se tiver h5 válido
    if pd.notna(h5):
        final_rows.append({
            'Conference' : nome_scholar,
            'Acronym' :  sigla,
            'Year' : 2025,
            'Topic' : 'General',
            'Papers(5Y)' : '',
            'Citations(5Y)' : '',
            'h5' : int(h5),
            'h5_source' : format_h5(2025, ['GS'])
        })
        included_names.add(nome_scholar)
        print(f"   -> OK! Found GS h5 = {h5}")
    else:
        # se estiver na lista de inclusão, tenta calcular via calc_h5
        if nome in manual_includes:
            print(f"   => INFO: CALCULATING h5 for {nome} using calc_h5.py...")
            h5_total, h5_med_total, total_citacoes, num_papers_total, fontes = run_h5_script(ANO_REF, sigla, PACOTE_DADOS)
            #print("IGNORANDO SCRIPT POR AGORA!")
            #h5_total = -1
            final_rows.append({
                'Conference' : nome,
                'Acronym' :  sigla,
                'Year' : 2025,
                'Topic' : 'General',
                'Papers(5Y)' : str(num_papers_total),
                'Citations(5Y)' : str(total_citacoes),
                'h5' : int(h5_total),
                'h5_source' : format_h5(2025, fontes)
            })
            included_names.add(nome)
            print(f"   -> Calculou h5 = {h5_total}")
        else:
            print(f"   -> Ignored! Not in {H5_MAIN_FILE} list or in {CONFS_LIST_FILE}: {nome}")

# --- 5. Incluir elementos da lista de inclusão que ainda não foram incluídos ---
for nome, sigla in manual_includes.items():
    if nome not in included_names:
        print(f"Incluindo manualmente {nome} (não estava no arquivo principal)")
        print("IGNORANDO SCRIPT POR AGORA! v2")
        h5_total = -1
        #h5_total, h5_med_total, _, _, _ = run_h5_script(ANO_REF, sigla, PACOTE_DADOS)
        final_rows.append({
            'Conference' : nome,
            'Acronym' :  sigla,
            'Year' : 2025,
            'Topic' : 'General',
            'Papers(5Y)' : '',
            'Citations(5Y)' : '',
            'h5' : int(h5_total),
            'h5_source' : format_h5(2025, ['GS'])
        })
        included_names.add(nome)

# --- 6. Gerar CSV ---
df_out = pd.DataFrame(final_rows)
df_out.to_csv(OUTPUT_FILE, index=False)
print(f"-------------------------------")
print(f"Generated Website CSV: {OUTPUT_FILE}")
print(f"-------------------------------")
print(f"github.com/cs-confs-br/cs-confs-br-data: END script generate_website.py")