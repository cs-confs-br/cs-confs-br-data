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
arquivos = ['SBPO_2019_2019_PPCR1_2025_09.csv', 
            'SBPO_2020_2020_PPCR1_2025_09.csv', 
            'SBPO_2021_2021_PPCR1_2025_09.csv', 
            'SBPO_2022_2022_PPCR1_2025_09.csv', 
            'SBPO_2023_2023_PPCR1_2025_09.csv',
            'SBPO_2024_2024_PPCR1_2025_09.csv']

ANO_REF = 2024
ANO_INICIO = ANO_REF-5
ANO_FIM = ANO_REF-1

# dataframe final agregado para o H5
dfs = []

print(f"github.com/cs-confs-br/cs-confs-br-data: executando script calc_h5.py")
print(f"Ano Referencia = {ANO_REF}, Ano Inicio = {ANO_INICIO}, Ano Fim = {ANO_FIM}")
print("=== H5 por ano ===")
for arq in arquivos:
    caminho = DATA_DIR + arq
    df_temp = pd.read_csv(caminho)  
    
    # filtra linhas no periodo desejado
    linhas_filtradas = []
    for _, row in df_temp.iterrows():
        ano = int(row["Year"])
        if ano >= ANO_INICIO and ano <= ANO_FIM:
            linhas_filtradas.append(row)
    
    if len(linhas_filtradas) == 0:
        print(f"{arq} -> H5 = {0}, Citacoes = {0}, Artigos = {0} [ignorado]...")
        continue
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
    print(f"{arq} -> H5 = {h5_ano}, Citacoes = {total_citacoes}, Artigos = {num_papers}")


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
print(f"H5 = {h5_total}, Total Citacoes = {total_citacoes}, Total Artigos = {num_papers_total}")


