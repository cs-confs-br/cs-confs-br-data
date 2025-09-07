import pandas as pd
import os

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

def carregar_openalex_csv(caminho):
    df_oa = pd.read_csv(caminho)

    # monta dataframe compatível com os outros
    df_conv = pd.DataFrame({
        "Title": df_oa["title"].fillna(df_oa["display_name"]),
        "Cites": df_oa["cited_by_count"].fillna(0).astype(int),
        "Authors": df_oa["authorships.author.display_name"].fillna(df_oa["authorships.raw_author_name"]).astype(str),
        "Year": df_oa["publication_year"].fillna(0).astype(int),
        "Source": df_oa["primary_location.source.display_name"].fillna(""),
        "Publisher": df_oa.get("primary_location.source.host_organization_name", pd.Series([""]*len(df_oa))),
        "DOI": df_oa["doi"].fillna(df_oa["ids.doi"])
    })

    # normaliza: separa autores por `|`
    df_conv["Authors"] = df_conv["Authors"].apply(lambda x: "; ".join(str(x).split("|")))

    return df_conv

def carrega_csv(caminho):
    if ".OA." in os.path.basename(caminho):
        print(f"Detectado OpenAlex: {caminho}")
        return carregar_openalex_csv(caminho)
    else:
        print(f"Detectado CSV padrão: {caminho}")
        return pd.read_csv(caminho)

#
DATA_DIR = '../data/SBPO/2025_09/'
arquivos = [
            'SBPO_2018_2024_OA_2025_09.OA.csv', 
            'SBPO_2019_2019_PPCR_2025_09.csv', 
            'SBPO_2020_2020_PPCR_2025_09.csv', 
            'SBPO_2021_2021_PPCR_2025_09.csv', 
            'SBPO_2022_2022_PPCR_2025_09.csv', 
            'SBPO_2023_2023_PPCR_2025_09.csv',
            'SBPO_2024_2024_PPCR_2025_09.csv',
            'SBPO_2019_2024_MAGS_2025_09.csv',
            ]

ANO_REF = 2024
ANO_INICIO = ANO_REF-5
ANO_FIM = ANO_REF-1

DEBUG=False

# dataframe final agregado para o H5
dfs = []

print(f"github.com/cs-confs-br/cs-confs-br-data: executando script calc_h5.py")
print(f"Ano Referencia = {ANO_REF}, Ano Inicio = {ANO_INICIO}, Ano Fim = {ANO_FIM}")
print("=== H-index por arquivo ===")
for arq in arquivos:
    caminho = DATA_DIR + arq
    df_temp = carrega_csv(caminho)  
    print(f"Processando {caminho}...")

    agrupados = df_temp.groupby("Title").size().reset_index(name="count")
    removidos = agrupados[agrupados["count"] > 1]
    if len(removidos) > 0:
        #print("WARNING: elementos repetidos!")
        #print(removidos)

        num_count1 = len(df_temp)
        df_temp = df_temp.groupby("Title", as_index=False).agg({
            "Cites": "max",
            "Authors": "first",
            "Year": "first",
        })
        num_count2 = len(df_temp)
        if num_count1 != num_count2:
            print(f"WARNING: removidos elementos num_count1={num_count1} num_count2={num_count2}")

    ano_min = +9999999
    ano_max = 0
    
    # filtra linhas no periodo desejado
    linhas_filtradas = []
    for _, row in df_temp.iterrows():
        ano = int(row["Year"])
        if ano < ano_min:
            ano_min = ano
        if ano > ano_max:
            ano_max = ano
        if ano >= ANO_INICIO and ano <= ANO_FIM:
            linhas_filtradas.append(row)
    
    if len(linhas_filtradas) == 0:
        print(f"H = {0}, Citacoes = {0}, Artigos = {0}, Periodo = [{ano_min}, {ano_max}], Limite = [{ANO_INICIO}, {ANO_FIM}] [ignorado]...")
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
    print(f"H = {h5_ano}, Citacoes = {total_citacoes}, Artigos = {num_papers}, Periodo = [{ano_min}, {ano_max}], Limite = [{ANO_INICIO}, {ANO_FIM}]")


print("Processando agregado final...")
df = pd.concat(dfs, ignore_index=True)

# TODO(igormcoelho): fazer função pra isso...
agrupados = df.groupby("Title").size().reset_index(name="count")
removidos = agrupados[agrupados["count"] > 1]
if len(removidos) > 0:
    print(f"WARNING: elementos repetidos! DEBUG={DEBUG}")
    if DEBUG:
        print(removidos)
    num_count1 = len(df)
    df = df.groupby("Title", as_index=False).agg({
        "Cites": "max",
        "Authors": "first",
        "Year": "first",
    })
    num_count2 = len(df)
    if num_count1 != num_count2:
        print(f"WARNING: removidos {num_count1-num_count2} elementos num_count1={num_count1} num_count2={num_count2}")

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
print(f"=== H5 ({ANO_REF}) ===")
print(f"H5 = {h5_total}, Total Citacoes = {total_citacoes}, Total Artigos = {num_papers_total}")


