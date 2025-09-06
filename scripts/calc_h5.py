import pandas as pd

# função recebe LISTA DE CITAÇÕES, numérica, e retorna h-index
def calcular_h_index(citacoes):
    citacoes = sorted(citacoes, reverse=True)
    h = 0
    for i, c in enumerate(citacoes, start=1):
        if c >= i:
            h = i
        else:
            break
    return h

#
DATA_DIR = '../data/SBPO/2025_09/'
arquivos = ['SBPO_2019_2019_PP1_2025_09.csv', 
            'SBPO_2020_2020_PP1_2025_09.csv', 
            'SBPO_2021_2021_PP1_2025_09.csv', 
            'SBPO_2022_2022_PP1_2025_09.csv', 
            'SBPO_2023_2023_PP1_2025_09.csv']

# dataframe final agregado para o H5
dfs = []

print("=== H5 por ano ===")
for arq in arquivos:
    caminho = DATA_DIR + arq
    df_temp = pd.read_csv(caminho)  
    
    # filtra linhas no periodo desejado
    linhas_filtradas = []
    for _, row in df_temp.iterrows():
        ano = int(row["Year"])
        if ano >= 2019 and ano <= 2024:
            linhas_filtradas.append(row)
    
    df_ano = pd.DataFrame(linhas_filtradas)
    dfs.append(df_ano) 

    total_citacoes = 0
    citacoes = []
    for valor in df_ano["Cites"].fillna(0):
        v = int(valor)
        # minimo de 1 citação para contribuir no fator H
        if v > 0:     
            citacoes.append(v)
            total_citacoes += v
    
    h5_ano = calcular_h_index(citacoes)
    num_papers = len(df_ano)
    print(f"{arq} -> H5 = {h5_ano}, Total de citações = {total_citacoes}, # Artigos = {num_papers}")


df = pd.concat(dfs, ignore_index=True)

# TODO(igormcoelho): fazer uma função pra isso
total_citacoes = 0
citacoes = []
for valor in df["Cites"].fillna(0):
    v = int(valor)
    # minimo de 1 citação para contribuir no fator H
    if v > 0:     
        citacoes.append(v)
        total_citacoes += v

h5_total = calcular_h_index(citacoes)
num_papers_total = len(df)
print("=== H5 Consolidado ===")
print(f"H5 = {h5_total}, Total de citações = {total_citacoes}, Papers = {num_papers_total}")


