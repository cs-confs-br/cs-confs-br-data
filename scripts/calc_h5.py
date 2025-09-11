import pandas as pd
import os
import statistics
import glob

# função recebe LISTA DE CITAÇÕES, numérica, e retorna h-index
def calcular_h_index(citacoes):
    citacoes = sorted(citacoes, reverse=True)
    h = 0
    for i, c in enumerate(citacoes, start=1):
        if c >= i:
            h = i
        else:
            break
    if h > 0:
        # print("hr_med list = ", citacoes[:h])
        h5_median = statistics.median(citacoes[:h])
    else:
        h5_median = 0
    
    return h, h5_median

def carregar_csv_padrao(caminho):
    df = pd.read_csv(caminho)

    df_conv = pd.DataFrame({
        "Title": df["Title"].fillna(""),
        "Cites": df["Cites"].fillna(0).astype(int),
        "Authors": df["Authors"].fillna("").astype(str),
        "Year": df["Year"].fillna(0).astype(int),
        "Source": df.get("Source", pd.Series([""]*len(df))),
        "Publisher": df.get("Publisher", pd.Series([""]*len(df))),
        "DOI": df.get("DOI", pd.Series([""]*len(df)))
    })

    return df_conv

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
        print(f"Processando {caminho}... [CSV OpenAlex]")
        return carregar_openalex_csv(caminho)
    else:
        print(f"Processando {caminho}... [CSV Padrão]")
        return carregar_csv_padrao(caminho)
    
def detectar_fonte(arq):
    # fontes disponíveis:
    # OA: OpenAlex
    # GS: Google Scholar
    # CR: Crossref
    # PPXX, onde XX é uma coleta via Publish or Perish do tipo XX
    # MAXX, onde XX é uma coleta MANUAL do tipo XX
    #
    # exemplos: 
    # - OA: coleta via OpenAlex em formato CSV padrão
    # - PPCR: coleta do crossref via publish or perish em formato CSV padrão
    # - MAGS.OA: coleta manual no google scholar em formato CSV OpenAlex
    #
    fontes = []
    if "_PPCR_" in arq:
        fontes.append("CR")
    if ".OA." in arq or "_OA_" in arq or "_PPOA_" in arq:
        fontes.append("OA")
    if "_PPGS_" in arq or "_MAGS_" in arq:
        fontes.append("GS")
    return fontes

def run_h5_script(ANO_REF, SIGLA, PACOTE):
    print(f"calc_h5.py => BEGIN run_h5_script({ANO_REF},{SIGLA},{PACOTE})")
    ANO_INICIO = ANO_REF-5
    ANO_FIM = ANO_REF-1
    print(f"Ano Referencia = {ANO_REF}, Ano Inicio = {ANO_INICIO}, Ano Fim = {ANO_FIM}")

    DEBUG=False
    DATA_DIR = f'../data/{SIGLA}/{PACOTE}/'

    # coleta arquivos CSV automaticamente, exceto os 'ignored/'
    arquivos = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    fontes = []
    print("Arquivos encontrados:")
    for arq in arquivos:
        fs = detectar_fonte(arq)
        fontes += fs
        print(" -", arq, "\tfontes: ", fs)
    # sem repetições...
    fontes = list(set(fontes))
    
    print("Fontes: ", fontes)


    # dataframe final agregado para o H5
    dfs = []

    print("=== H-index por arquivo ===")
    for arq in arquivos:
        caminho = arq
        df_temp = carrega_csv(caminho)

        if DEBUG:
            print(df_temp.head(10))

        # ignorando campos sem Year... colocando ANO_INICIO - 1
        df_temp["Year"] = pd.to_numeric(df_temp["Year"], errors="coerce").fillna(ANO_INICIO-1).astype(int)

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
                print(f"WARNING: removidos {num_count1-num_count2} elementos num_count1={num_count1} num_count2={num_count2}")

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
            print(f"H = {0}, Hmed = {0}, Citacoes = {0}, Artigos = {0}, Periodo = [{ano_min}, {ano_max}], Limite = [{ANO_INICIO}, {ANO_FIM}] [ignorado]...")
            continue
        else:
            # print(f"Entradas = {len(linhas_filtradas)}")
            pass
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
        if DEBUG:
            print(f"max_cite = {max(citacoes)}")
            print(citacoes)
        h5_ano, h5_med_ano = calcular_h_index(citacoes)
        num_papers = len(df_ano)
        print(f"H = {h5_ano}, Hmed = {h5_med_ano}, Citacoes = {total_citacoes}, Artigos = {num_papers}, Periodo = [{ano_min}, {ano_max}], Limite = [{ANO_INICIO}, {ANO_FIM}]")


    print("Processando agregado final...")
    df = pd.concat(dfs, ignore_index=True)

    # TODO(igormcoelho): fazer função pra isso...
    agrupados = df.groupby("Title").size().reset_index(name="count")
    removidos = agrupados[agrupados["count"] > 1]
    if len(removidos) > 0:
        # print(f"WARNING: elementos repetidos! DEBUG={DEBUG}")
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

    h5_total, h5_med_total = calcular_h_index(citacoes)
    num_papers_total = len(df)
    print(f"=== H5 ({ANO_REF}) ===")
    print(f"H5 = {h5_total}, H5med = {h5_med_total}, Total Citacoes = {total_citacoes}, Total Artigos = {num_papers_total}")
    print(f"calc_h5.py => END run_h5_script({ANO_REF},{SIGLA},{PACOTE})")
    return h5_total, h5_med_total, total_citacoes, num_papers_total, fontes

if __name__ == '__main__':
    print(f"github.com/cs-confs-br/cs-confs-br-data: executando script calc_h5.py")
    h5_total, h5_med_total, total_citacoes, num_papers_total, fontes = run_h5_script(2024, 'SBPO', '2025_09')
    print("Fontes utilizadas:", fontes)