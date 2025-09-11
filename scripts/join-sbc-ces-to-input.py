import pandas as pd
import glob
import os
import re

def extract_gs_id(url):
    if pd.isna(url) or not isinstance(url, str) or not url.strip():
        return ""
    m = re.search(r"venue=([^&\s]+)", url)
    if not m:
        return ""
    gs = m.group(1).strip()
    # remove sufixo de ano .2020, .2021 etc (se existir)
    gs = re.sub(r'\.\d{4}$', '', gs)
    return gs

def extract_dblp_id(url):
    if pd.isna(url) or not isinstance(url, str):
        return ""
    m = re.search(r"/db/conf/([^/]+)/", url)
    return m.group(1) if m else ""

def extract_sol_id(url):
    """
    Extrai o ID de um link da SOL.
    Exemplo: https://sol.sbc.org.br/index.php/bresci -> bresci
    """
    if pd.isna(url) or not isinstance(url, str) or not url.strip():
        return ""
    m = re.search(r"/index\.php/([^/?#]+)", url)
    return m.group(1) if m else ""

def normalize_avaliacao(val):
    if not isinstance(val, str):
        return ""
    v = val.strip().lower().replace(" ", "")
    if v.startswith("top10"):
        return "Top10"
    elif v.startswith("top20"):
        return "Top20"
    elif v.startswith("eventos"):
        return "Recommended"
    else:
        print(f"WARNING: normalize_avaliacao val = {val}")
        return "Recommended"

# arquivos CSV de CE
files = glob.glob("../sbc/CE-2024/SBC-CE-*.csv")

# carregar planilha mestre
df_master = pd.read_csv("../cs-confs-br-list.csv")

for col in ["SBC-CE", "Nomes Alternativos", "Avaliação SBC", "GS ID", "DBLP ID", "SOL ID"]:
    if col in df_master.columns:
        df_master[col] = df_master[col].astype("string").fillna("")

# índice por Sigla para facilitar merge
df_master.set_index("Sigla", inplace=True)

# índice auxiliar case-insensitive para siglas principais
master_siglas_insensitive = {k.strip().casefold(): k for k in df_master.index}

# índice auxiliar case-insensitive para siglas alternativas
alt_siglas_insensitive = {}
for sigla_principal, row in df_master.iterrows():
    alt_str = str(row.get("Siglas Alternativas", ""))
    for alt in alt_str.split("|"):
        alt = alt.strip()
        if alt:
            alt_siglas_insensitive[alt.casefold()] = sigla_principal

def get_sigla_principal(sigla):
    sigla_key = sigla.strip().casefold()
    if sigla_key in master_siglas_insensitive:
        return master_siglas_insensitive[sigla_key]
    if sigla_key in alt_siglas_insensitive:
        return alt_siglas_insensitive[sigla_key]
    return None

count_ok = 0
count_not_ok = 0
dic_ce_top10 = {}
dic_ce_top20 = {}
dic_ce_rec = {}
for f in files:
    print(f"Processando '{f}'...")
    ce = os.path.basename(f).replace("SBC-", "").replace("-2024.csv", "")
    print(f"CE = {ce}")
    df_ce = pd.read_csv(f)

    for _, row in df_ce.iterrows():
        sigla = str(row["SIGLA"]).strip()
        nome = str(row["NOME"]).strip()
        alt_nome = str(row.get("Nome Alternativo", "")).strip()
        top = str(row["TOP"]).strip() if "TOP" in row else ""
        top = normalize_avaliacao(top)
        gs_id = extract_gs_id(row.get("GOOGLE METRICS LINK", ""))
        dblp_id = extract_dblp_id(row.get("Link da DBLP", ""))
        #sol_link = str(row.get("Link da SOL", "")).strip()
        sol_id = extract_sol_id(row.get("Link da SOL", ""))

        if top == "Top10":
            dic_ce_top10[ce] = dic_ce_top10.get(ce, 0) + 1
        elif top == "Top20":
            dic_ce_top20[ce] = dic_ce_top20.get(ce, 0) + 1
        else:
            dic_ce_rec[ce] = dic_ce_rec.get(ce, 0) + 1

        #sigla_key = sigla.casefold()

        sigla_real = get_sigla_principal(sigla)
        if sigla_real:

            count_ok += 1
            ev = df_master.loc[sigla_real]

            # atualiza cadastro de origens
            origens = set(str(ev.get("Origem Cadastro", "")).split("|")) if pd.notna(ev.get("Origem Cadastro", "")) else set()
            origens.add("SBC CE-2024")
            df_master.at[sigla_real, "Origem Cadastro"] = "|".join(sorted(x for x in origens if x))

            # nomes alternativos (evita repetição)
            nomes_alt = set(str(ev.get("Nomes Alternativos", "")).split("|")) if pd.notna(ev.get("Nomes Alternativos", "")) else set()
            if nome != ev["Nome do evento"]:
                nomes_alt.add(nome)
            if alt_nome:
                nomes_alt.add(alt_nome)
            df_master.at[sigla_real, "Nomes Alternativos"] = "|".join(sorted(x for x in nomes_alt if x))

            # avaliação (agregar em vez de sobrescrever)
            avaliacoes = set(str(ev.get("Avaliação SBC", "")).split("|")) if pd.notna(ev.get("Avaliação SBC", "")) else set()
            if top:
                avaliacoes.add(top)

            # aplicar hierarquia: Top10 > Top20 > Recommended
            if "Top10" in avaliacoes:
                final_avaliacao = "Top10"
            elif "Top20" in avaliacoes:
                final_avaliacao = "Top20"
            elif "Recommended" in avaliacoes:
                final_avaliacao = "Recommended"
            else:
                final_avaliacao = ""

            df_master.at[sigla_real, "Avaliação SBC"] = final_avaliacao

            # SBC-CE
            ces = set(str(ev.get("SBC-CE", "")).split("|")) if pd.notna(ev.get("SBC-CE", "")) else set()
            ces.add(ce)
            df_master.at[sigla_real, "SBC-CE"] = "|".join(sorted(x for x in ces if x))

            # GS/DBLP
            if not ev.get("GS ID") and gs_id:
                df_master.at[sigla_real, "GS ID"] = gs_id
            if not ev.get("DBLP ID") and dblp_id:
                df_master.at[sigla_real, "DBLP ID"] = dblp_id
            if not ev.get("SOL ID") and sol_id:
                df_master.at[sigla_real, "SOL ID"] = sol_id


        else:
            count_not_ok += 1
            print(f"WARNING: novo evento encontrado ce='{ce}' sigla='{sigla}' nome='{nome}' alt_nome='{alt_nome}'")
            '''
            df_master.loc[sigla] = {
                "Nome do evento": nome,
                "Siglas Alternativas": "",
                "Nomes Alternativos": alt_nome if alt_nome else "",
                "Sociedade": "",
                "Avaliação SBC": top,
                "SBC-CE": ce,
                "Anais": "",
                "SOL Link": sol_link,
                "GS Link": gs_id,
                "DBLP Link": dblp_id
            }
            '''

print(f"OK = {count_ok};  NOT OK = {count_not_ok}")
print(f"top10 = {dic_ce_top10}")
print(f"top20 = {dic_ce_top20}")
print(f"rec len = {len(dic_ce_rec)}")


# salvar planilha atualizada
df_master.reset_index(inplace=True)
df_master.to_csv("../cs-confs-br-list-updated.csv", index=False)
print("Planilha atualizada (cópia)")