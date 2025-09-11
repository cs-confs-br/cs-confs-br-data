import pandas as pd
import glob
import os
import re


def get_sigla_principal(sigla, lmaster, lalt):
    sigla_key = sigla.strip().casefold()
    if sigla_key in lmaster:
        return lmaster[sigla_key], True
    if sigla_key in lalt:
        return lalt[sigla_key], False
    return None, None

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


# ============= ETAPA 1 =============
#     Correcao de Nomes e Siglas
# ===================================

def adiciona_nome_sigla_alternativos(sigla_real, df_ref, alt_nome, alt_sigla):
    ev = df_ref.loc[sigla_real]
    mudou = False
    #
    nomes_alt = set(str(ev.get("Nomes Alternativos", "")).split("|")) if pd.notna(ev.get("Nomes Alternativos", "")) else set()
    tam = len(nomes_alt)
    if alt_nome:
        nomes_alt.add(alt_nome)
        if len(nomes_alt) != tam:
            mudou = True
    df_ref.at[sigla_real, "Nomes Alternativos"] = "|".join(sorted(x for x in nomes_alt if x))
    #
    siglas_alt = set(str(ev.get("Siglas Alternativas", "")).split("|")) if pd.notna(ev.get("Siglas Alternativas", "")) else set()
    tam = len(siglas_alt)
    if alt_sigla:
        siglas_alt.add(alt_sigla)
        if len(siglas_alt) != tam:
            mudou = True
    df_master.at[sigla_real, "Siglas Alternativas"] = "|".join(sorted(x for x in siglas_alt if x))
    return mudou

def corrige_nomes(df_main, ce_files, cria_novas = False):
    # índice auxiliar case-insensitive para siglas principais
    lmain = {k.strip().casefold(): k for k in df_main.index}

    # índice auxiliar case-insensitive para siglas alternativas
    lalt  = {}
    for sigla_principal, row in df_main.iterrows():
        alt_str = str(row.get("Siglas Alternativas", ""))
        for alt in alt_str.split("|"):
            alt = alt.strip()
            if alt:
                lalt[alt.casefold()] = sigla_principal

    count_changes = 0
    count_novas = 0
    for f in ce_files:
        #print(f"Processando '{f}'...")
        ce = os.path.basename(f).replace("SBC-", "").replace("-2024.csv", "")
        #print(f"CE = {ce}")
        df_ce = pd.read_csv(f)

        for _, row in df_ce.iterrows():
            sigla = str(row["SIGLA"]).strip()
            nome = row["NOME"] if pd.notna(row["NOME"]) else ""
            alt_nome = row.get("Nome Alternativo", "")
            alt_nome = alt_nome if pd.notna(alt_nome) else ""
            novo_nome = row.get("Novo Nome", "")
            novo_nome = novo_nome if pd.notna(novo_nome) else ""
            nova_sigla = row.get("Nova Sigla", "")
            nova_sigla = nova_sigla if pd.notna(nova_sigla) else ""
            if nova_sigla != "" or novo_nome != "" or alt_nome != "":
                #print(f"sigla = '{sigla}'  nome = '{nome}' nova_sigla = '{nova_sigla}' novo_nome = '{novo_nome}' alt_nome = '{alt_nome}'")

                sigla_real, is_main = get_sigla_principal(sigla, lmain, lalt)
                mudou = False
                if sigla_real:
                    if novo_nome != "" or nova_sigla != "":
                        #print("Passo 1")
                        m1 = adiciona_nome_sigla_alternativos(sigla_real, df_main, novo_nome, nova_sigla)
                        mudou = mudou or m1
                    if alt_nome != "":
                        #print("Passo 2")
                        m2 = adiciona_nome_sigla_alternativos(sigla_real, df_main, alt_nome, "")
                        mudou = mudou or m2
                    if mudou:
                        print(f"Mudança em sigla = '{sigla}'")
                        count_changes += 1
                else:
                    # nova conferencia!
                    print(f"NOVA CONFERENCIA!  sigla_real = '{sigla_real}' sigla = '{sigla}' nome = '{nome}'")
                    count_novas += 1
                    if cria_novas and sigla != "":
                        print(f"CRIANDO...")
                        # monta linha com valores básicos
                        new_row = {
                            "Ano Dados": 2025,
                            "Sigla": sigla,
                            "Nome do evento": nome,
                            "Siglas Alternativas": "",
                            "Nomes Alternativos": "",
                            "Sociedade": "",
                            "Avaliação SBC": "",
                            "SBC-CE": ce,
                            "Origem Cadastro": "SBC CE-2024",
                            "Anais": "",
                            "SOL ID": "",
                            "GS ID": "",
                            "DBLP ID": "",
                        }
                        df_main.loc[sigla] = new_row
                        # atualiza índices auxiliares
                        lmain[sigla.casefold()] = sigla
                    pass
    print(f"count_changes = {count_changes}")
    print(f"count_novas = {count_novas}")
    return count_changes, count_novas


def altera_sigla_primaria(df_main, ce_files):
    # índice auxiliar case-insensitive para siglas principais
    lmain = {k.strip().casefold(): k for k in df_main.index}

    # índice auxiliar case-insensitive para siglas alternativas
    lalt  = {}
    for sigla_principal, row in df_main.iterrows():
        alt_str = str(row.get("Siglas Alternativas", ""))
        for alt in alt_str.split("|"):
            alt = alt.strip()
            if alt:
                lalt[alt.casefold()] = sigla_principal

    count_changes = 0
    for f in ce_files:
        print(f"Processando '{f}'...")
        ce = os.path.basename(f).replace("SBC-", "").replace("-2024.csv", "")
        print(f"CE = {ce}")
        df_ce = pd.read_csv(f)

        for _, row in df_ce.iterrows():
            sigla = str(row["SIGLA"]).strip()
            nome = row["NOME"] if pd.notna(row["NOME"]) else ""
            novo_nome = row.get("Novo Nome", "")
            novo_nome = novo_nome if pd.notna(novo_nome) else ""
            nova_sigla = row.get("Nova Sigla", "")
            nova_sigla = nova_sigla if pd.notna(nova_sigla) else ""
            if nova_sigla != "":
                #print(f"altera_sigla_primaria sigla = '{sigla}'  nome = '{nome}' => nova_sigla = '{nova_sigla}' novo_nome = '{novo_nome}'")

                sigla_real, is_main = get_sigla_principal(nova_sigla, lmain, lalt)
                #print(f"consulta: nova_sigla = '{nova_sigla}' => sigla_real = {sigla_real} principal(T) ou alternativo(F) = '{is_main}' ")
                if sigla_real and is_main:
                    #print(f"WARNING: sigla nova já existe e é primária! nova_sigla = '{nova_sigla}'")
                    #print("NADA A FAZER!")
                    pass
                elif sigla_real and not is_main:
                    print(f"IMPORTANTE! Precisa trocar sigla! nova_sigla = '{nova_sigla}' sigla_real = '{sigla_real}'")
                    sigla_antiga = sigla_real
                    nome_antigo = nome
                    linha = df_main.loc[sigla_antiga].copy()
                    df_main = df_main.drop(sigla_antiga)
                    df_main.loc[nova_sigla] = linha
                    sigla_real = nova_sigla
                    m1 = adiciona_nome_sigla_alternativos(sigla_real, df_main, nome_antigo, sigla_antiga)
                    if m1:
                        count_changes += 1
                pass
    print(f"count_changes = {count_changes}")
    return count_changes



# arquivos CSV de CE
files = glob.glob("../sbc/CE-2024/SBC-CE-*.csv")

# carregar planilha mestre
df_master = pd.read_csv("../cs-confs-br-list.csv")

#for col in ["SBC-CE", "Nomes Alternativos", "Siglas Alternativas", "Avaliação SBC", "GS ID", "DBLP ID", "SOL ID"]:
#    if col in df_master.columns:
#        df_master[col] = df_master[col].astype("string").fillna("")

# índice por Sigla para facilitar merge
df_master.set_index("Sigla", inplace=True)


print("ROUND 1")
r1_c1, r1_c2 = corrige_nomes(df_master, files, False)
print(f"mudanças = {r1_c1}  novas = {r1_c2} (SEM CRIAR NOVAS)")
#
print("ROUND 2")
r2_c1,r2_c2 = corrige_nomes(df_master, files, True)
print(f"mudanças = {r2_c1}  novas = {r2_c2} (CRIANDO NOVAS)")
#
print("ROUND 3")
r3_c1,r3_c2 = corrige_nomes(df_master, files, False)
print(f"mudanças = {r3_c1}  novas = {r3_c2} (SEM CRIAR NOVAS)")
assert(r3_c2 == 0)
#
print("ROUND 4")
r4_c1,r4_c2 = corrige_nomes(df_master, files, False)
print(f"mudanças = {r4_c1}  novas = {r4_c2} (SEM CRIAR NOVAS) -> Verificação final!")
assert(r4_c1 == 0)
assert(r4_c2 == 0)
#
print("ROUND 5")
r5_c1 = altera_sigla_primaria(df_master, files)
print(f"mudanças = {r5_c1} (ALTERACOES DE NOMES)")
#
print("ROUND 6")
r6_c1,r6_c2 = corrige_nomes(df_master, files, False)
print(f"mudanças = {r6_c1}  novas = {r6_c2} (SEM CRIAR NOVAS) -> Verificação final FINAL!")
assert(r6_c1 == 0)
assert(r6_c2 == 0)
#
print("========== RESUMO ETAPA 1 ============")
print(f"mudanças = {r1_c1}  novas = {r1_c2} (SEM CRIAR NOVAS)")
print(f"mudanças = {r2_c1}  novas = {r2_c2} (CRIANDO NOVAS)")
print(f"mudanças = {r3_c1}  novas = {r3_c2} (SEM CRIAR NOVAS)")
print(f"mudanças = {r4_c1}  novas = {r4_c2} (SEM CRIAR NOVAS) -> Verificação final!")
print(f"mudanças = {r5_c1} (ALTERACOES DE NOMES)")
print(f"mudanças = {r6_c1}  novas = {r6_c2} (SEM CRIAR NOVAS) -> Verificação final FINAL!")
print("======================================")


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
        nome = row["NOME"] if pd.notna(row["NOME"]) else ""
        alt_nome = row.get("Nome Alternativo", "")
        alt_nome = alt_nome if pd.notna(alt_nome) else ""
        novo_nome = row.get("Novo Nome", "")
        novo_nome = novo_nome if pd.notna(novo_nome) else ""
        nova_sigla = row.get("Nova Sigla", "")
        nova_sigla = nova_sigla if pd.notna(nova_sigla) else ""
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

        sigla_real, is_main = get_sigla_principal(sigla, master_siglas_insensitive, alt_siglas_insensitive)
        if sigla_real:

            count_ok += 1
            ev = df_master.loc[sigla_real]

            # atualiza cadastro de origens
            origens = set(str(ev.get("Origem Cadastro", "")).split("|")) if pd.notna(ev.get("Origem Cadastro", "")) else set()
            origens.add("SBC CE-2024")
            df_master.at[sigla_real, "Origem Cadastro"] = "|".join(sorted(x for x in origens if x))

            adiciona_nome_sigla_alternativos(sigla_real, df_master, nome, sigla)
            adiciona_nome_sigla_alternativos(sigla_real, df_master, alt_nome, nova_sigla)
            
            '''
            # nomes alternativos (evita repetição)
            nomes_alt = set(str(ev.get("Nomes Alternativos", "")).split("|")) if pd.notna(ev.get("Nomes Alternativos", "")) else set()
            if nome != ev["Nome do evento"]:
                nomes_alt.add(nome)
            if alt_nome:
                nomes_alt.add(alt_nome)
            df_master.at[sigla_real, "Nomes Alternativos"] = "|".join(sorted(x for x in nomes_alt if x))

            # siglas alternativas (atualiza incluindo a Nova Sigla)
            siglas_alt = set(str(ev.get("Siglas Alternativas", "")).split("|")) if pd.notna(ev.get("Siglas Alternativas", "")) else set()
            if sigla_real != sigla:
                siglas_alt.add(sigla)
            nova_sigla = str(row.get("Nova Sigla", "")).strip()
            if nova_sigla and sigla_real != nova_sigla:
                siglas_alt.add(nova_sigla)
            df_master.at[sigla_real, "Siglas Alternativas"] = "|".join(sorted(x for x in siglas_alt if x))
            '''

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
            pass

print(f"OK = {count_ok};  NOT OK = {count_not_ok}")
print(f"top10 = {dic_ce_top10}")
print(f"top20 = {dic_ce_top20}")
print(f"rec len = {len(dic_ce_rec)}")


# salvar planilha atualizada
df_master.reset_index(inplace=True)
# df_master.to_csv("../cs-confs-br-list-updated.csv", index=False)
df_master.to_csv("../cs-confs-br-list-updated.csv", index=False, na_rep="")

print("Planilha atualizada (cópia)")