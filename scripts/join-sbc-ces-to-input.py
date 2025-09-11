import pandas as pd
import glob
import os
import re

# arquivos CSV de CE
files = glob.glob("../sbc/CE-2024/SBC-CE-*.csv")

# carregar planilha mestre
df_master = pd.read_csv("../cs-confs-br-list.csv")

for col in ["SBC-CE", "Nomes Alternativos", "Avaliação SBC", "GS Link", "DBLP Link", "Anais Link"]:
    if col in df_master.columns:
        df_master[col] = df_master[col].astype("string").fillna("")

# índice por Sigla para facilitar merge
df_master.set_index("Sigla", inplace=True)

def extract_gs_id(url):
    if pd.isna(url) or not isinstance(url, str) or not url.strip():
        return ""
    m = re.search(r"venue=([^&]+)", url)
    if not m:
        return ""
    gs = m.group(1).strip()
    gs = re.sub(r'\.\d{4}$', '', gs)   # remove .2023, .2024, etc.
    return gs

def extract_dblp_id(url):
    if pd.isna(url) or not isinstance(url, str):
        return ""
    m = re.search(r"/db/conf/([^/]+)/", url)
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
        sol_link = str(row.get("Link da SOL", "")).strip()

        if top == "Top10":
            dic_ce_top10[ce] = dic_ce_top10.get(ce, 0) + 1
        elif top == "Top20":
            dic_ce_top20[ce] = dic_ce_top20.get(ce, 0) + 1
        else:
            dic_ce_rec[ce] = dic_ce_rec.get(ce, 0) + 1

        if sigla in df_master.index:
            count_ok += 1
            ev = df_master.loc[sigla]

            # nomes alternativos (evita repetição)
            nomes_alt = set(str(ev.get("Nomes Alternativos", "")).split("|")) if pd.notna(ev.get("Nomes Alternativos", "")) else set()
            if nome != ev["Nome do evento"]:
                nomes_alt.add(nome)
            if alt_nome:
                nomes_alt.add(alt_nome)
            df_master.at[sigla, "Nomes Alternativos"] = "|".join(sorted(x for x in nomes_alt if x))

            # avaliação
            if pd.isna(ev.get("Avaliação SBC")) or ev["Avaliação SBC"] == "":
                df_master.at[sigla, "Avaliação SBC"] = top

            # SBC-CE
            ces = set(str(ev.get("SBC-CE", "")).split("|")) if pd.notna(ev.get("SBC-CE", "")) else set()
            ces.add(ce)
            df_master.at[sigla, "SBC-CE"] = "|".join(sorted(x for x in ces if x))

            # GS/DBLP
            if not ev.get("GS Link") and gs_id:
                df_master.at[sigla, "GS Link"] = gs_id
            if not ev.get("DBLP Link") and dblp_id:
                df_master.at[sigla, "DBLP Link"] = dblp_id
            if not ev.get("Anais Link") and sol_link:
                df_master.at[sigla, "Anais Link"] = sol_link


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
                "Anais Link": sol_link,
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