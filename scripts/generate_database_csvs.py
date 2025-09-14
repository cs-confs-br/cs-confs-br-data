#!/usr/bin/env python3
import pandas as pd
import json
import glob
import os
import re
from datetime import datetime

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'config.json')
    if not os.path.exists(config_path):
        config_path = '../../../config.json'
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_rename_rules():
    rules_path = os.path.join(os.path.dirname(__file__), 'rename_rules.json')
    with open(rules_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def normalize_column_name(df, possible_names):
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def extract_id_from_url(url, pattern):
    if not url or pd.isna(url):
        return ''
    match = re.search(pattern, str(url))
    return match.group(1) if match else ''

def extract_numeric_id(comp_id):
    if comp_id and comp_id.startswith('CompID'):
        return int(comp_id[6:])
    return 0

def load_qualis(config):
    path = config['data_sources']['qualis_2017']['path']

    if path.startswith('../'):
        path = os.path.join(os.path.dirname(__file__), '..', path[3:])

    if not os.path.exists(path):
        path = os.path.join(os.path.dirname(__file__), '..', 'cs-confs-br-list-updated.site.csv')

    if not os.path.exists(path):
        raise FileNotFoundError(f"Qualis dataset not found at {path}")

    return pd.read_csv(path, encoding='utf-8')

def load_ce_files(config):
    ce_config = config['data_sources']['ce_2024']
    path = ce_config['path']
    pattern = ce_config['file_pattern']

    if path.startswith('../'):
        path = os.path.join(os.path.dirname(__file__), '..', path[3:])
    else:
        path = os.path.join(os.path.dirname(__file__), '..', path)

    search_paths = [
        os.path.join(path, pattern)
    ]

    for search_path in search_paths:
        files = glob.glob(search_path)
        if files:
            return files

    return []

def process_qualis(df, rename_rules):
    processed = []
    duplicates_to_remove = set()

    qualis_dups = rename_rules.get('qualis_duplicates', {}).get('unify', {})
    for sigla, dup_info in qualis_dups.items():
        for comp_id in dup_info.get('remove', []):
            duplicates_to_remove.add(comp_id)

    for idx, row in df.iterrows():
        sigla = row.get('Sigla', '')
        if not sigla or pd.isna(sigla):
            continue

        comp_id = row.get('ID Conferencia', '')
        if not comp_id or pd.isna(comp_id):
            continue

        if comp_id in duplicates_to_remove:
            print(f"  Removing Qualis duplicate: {comp_id} ({sigla})")
            continue

        processed.append({
            'comp_id': comp_id,
            'sigla': sigla,
            'nome': row.get('Nome do evento', ''),
            'siglas_alt': row.get('Siglas Alternativas', ''),
            'nomes_alt': row.get('Nomes Alternativos', ''),
            'origem': row.get('Origem Cadastro', 'Qualis 2017'),
            'avaliacao': row.get('Avaliação SBC', ''),
            'gs_id': row.get('GS ID', ''),
            'dblp_id': row.get('DBLP ID', ''),
            'sociedade': row.get('Sociedade', ''),
            'ano_dados': row.get('Ano Dados', '')
        })

    return pd.DataFrame(processed)

def should_apply_rename(sigla, nova_sigla, rename_rules, existing_siglas):
    if not nova_sigla or nova_sigla == sigla:
        return False

    ignore_list = rename_rules.get('ignore_renames', {})
    if sigla in ignore_list and ignore_list[sigla].get('wrong_rename') == nova_sigla:
        return False

    force_list = rename_rules.get('force_renames', {})
    if sigla in force_list and force_list[sigla].get('new_name') == nova_sigla:
        return True

    unifications = rename_rules.get('unifications', {})
    for unified_name, rules in unifications.items():
        if nova_sigla == unified_name and sigla in rules.get('absorbs', []):
            return True

    return True

def handle_unification(sigla, nova_sigla, rename_rules):
    unifications = rename_rules.get('unifications', {})
    for unified_name, rules in unifications.items():
        if nova_sigla == unified_name and sigla in rules.get('absorbs', []):
            return True, unified_name
    return False, None

def process_ce_file(filepath, existing_siglas, rename_rules, next_comp_id):
    df = pd.read_csv(filepath, encoding='utf-8')
    ce_name = os.path.basename(filepath).replace('.ce.csv', '').replace('SBC-', '')

    new_conferences = []
    updates = {}
    unifications = {}

    sigla_col = normalize_column_name(df, ['SIGLA', 'Sigla', 'sigla'])
    nome_col = normalize_column_name(df, ['NOME', 'Nome', 'nome', 'Nome do evento'])
    gs_col = normalize_column_name(df, ['GOOGLE METRICS LINK', 'Link GS'])
    dblp_col = normalize_column_name(df, ['Link da DBLP', 'Link DBLP'])
    top_col = normalize_column_name(df, ['TOP', 'Top'])
    nova_sigla_col = normalize_column_name(df, ['Nova Sigla', 'nova sigla'])
    novo_nome_col = normalize_column_name(df, ['Novo Nome', 'novo nome'])

    if not sigla_col:
        return new_conferences, updates, unifications, next_comp_id

    for idx, row in df.iterrows():
        sigla = str(row[sigla_col]).strip() if pd.notna(row[sigla_col]) else ''
        if not sigla or sigla == 'nan':
            continue

        nome = row[nome_col] if nome_col and pd.notna(row.get(nome_col)) else ''
        gs_url = row[gs_col] if gs_col and pd.notna(row.get(gs_col)) else ''
        dblp_url = row[dblp_col] if dblp_col and pd.notna(row.get(dblp_col)) else ''
        top = row[top_col] if top_col and pd.notna(row.get(top_col)) else ''
        nova_sigla = row[nova_sigla_col] if nova_sigla_col and pd.notna(row.get(nova_sigla_col)) else ''
        novo_nome = row[novo_nome_col] if novo_nome_col and pd.notna(row.get(novo_nome_col)) else ''

        gs_id = extract_id_from_url(gs_url, r'venue=([^&\s]+)')
        dblp_id = extract_id_from_url(dblp_url, r'/db/conf/([^/]+)/')

        if sigla in existing_siglas:
            if nova_sigla and should_apply_rename(sigla, nova_sigla, rename_rules, existing_siglas):
                is_unification, unified_name = handle_unification(sigla, nova_sigla, rename_rules)
                if is_unification:
                    if unified_name not in unifications:
                        unifications[unified_name] = []
                    unifications[unified_name].append(sigla)
                else:
                    updates[sigla] = {
                        'nova_sigla': nova_sigla,
                        'novo_nome': novo_nome if novo_nome else nome,
                        'gs_id': gs_id,
                        'dblp_id': dblp_id,
                        'avaliacao': top
                    }
            else:
                updates[sigla] = {
                    'gs_id': gs_id,
                    'dblp_id': dblp_id,
                    'avaliacao': top
                }
        else:
            comp_id = f"CompID8{str(next_comp_id).zfill(5)}"
            next_comp_id += 1

            use_sigla = sigla
            use_nome = nome
            alt_sigla = ''
            alt_nome = ''

            if nova_sigla and not should_apply_rename(sigla, nova_sigla, rename_rules, existing_siglas):
                nova_sigla = ''

            if nova_sigla:
                use_sigla = nova_sigla
                alt_sigla = sigla

            if novo_nome:
                use_nome = novo_nome
                alt_nome = nome

            new_conferences.append({
                'comp_id': comp_id,
                'sigla': use_sigla,
                'nome': use_nome,
                'siglas_alt': alt_sigla,
                'nomes_alt': alt_nome,
                'origem': f'SBC-CE-{ce_name}',
                'avaliacao': top,
                'gs_id': gs_id,
                'dblp_id': dblp_id,
                'sociedade': 'SBC',
                'ano_dados': ''
            })

    return new_conferences, updates, unifications, next_comp_id

def apply_unifications(df, all_unifications, rename_rules):
    for unified_name, siglas_to_unify in all_unifications.items():
        if not siglas_to_unify:
            continue

        target_mask = df['sigla'] == unified_name
        if not target_mask.any():
            first_sigla = siglas_to_unify[0]
            target_mask = df['sigla'] == first_sigla
            if target_mask.any():
                df.loc[target_mask, 'sigla'] = unified_name

        for sigla in siglas_to_unify:
            if sigla == unified_name:
                continue

            source_mask = df['sigla'] == sigla
            if source_mask.any() and target_mask.any():
                source_row = df.loc[source_mask].iloc[0]

                current_alt = df.loc[target_mask, 'siglas_alt'].values[0]
                if current_alt and not pd.isna(current_alt):
                    df.loc[target_mask, 'siglas_alt'] = f"{current_alt}|{sigla}"
                else:
                    df.loc[target_mask, 'siglas_alt'] = sigla

                source_alt = source_row['siglas_alt']
                if source_alt and not pd.isna(source_alt):
                    current = df.loc[target_mask, 'siglas_alt'].values[0]
                    df.loc[target_mask, 'siglas_alt'] = f"{current}|{source_alt}"

                if source_row['gs_id'] and not pd.isna(source_row['gs_id']):
                    if pd.isna(df.loc[target_mask, 'gs_id'].values[0]):
                        df.loc[target_mask, 'gs_id'] = source_row['gs_id']

                if source_row['dblp_id'] and not pd.isna(source_row['dblp_id']):
                    if pd.isna(df.loc[target_mask, 'dblp_id'].values[0]):
                        df.loc[target_mask, 'dblp_id'] = source_row['dblp_id']

                df = df[~source_mask]

    return df

def merge_all(qualis_df, ce_files, config, rename_rules):
    result = qualis_df.copy()
    existing_siglas = set(result['sigla'].values)

    all_new = []
    all_updates = {}
    all_unifications = {}
    next_comp_id = 1

    ce_total = 0
    duplicates_found = 0

    for ce_file in ce_files:
        new_confs, updates, unifications, next_comp_id = process_ce_file(
            ce_file, existing_siglas, rename_rules, next_comp_id
        )

        ce_df = pd.read_csv(ce_file, encoding='utf-8')
        ce_total += len(ce_df[ce_df['SIGLA'].notna()])
        duplicates_found += len(updates)

        all_new.extend(new_confs)
        all_updates.update(updates)

        for unified_name, siglas in unifications.items():
            if unified_name not in all_unifications:
                all_unifications[unified_name] = []
            all_unifications[unified_name].extend(siglas)

        for conf in new_confs:
            existing_siglas.add(conf['sigla'])

    if all_new:
        new_df = pd.DataFrame(all_new)
        result = pd.concat([result, new_df], ignore_index=True)

    for sigla, update_info in all_updates.items():
        mask = result['sigla'] == sigla
        if not mask.any():
            continue

        if 'nova_sigla' in update_info and update_info['nova_sigla']:
            old_sigla = result.loc[mask, 'sigla'].values[0]
            old_nome = result.loc[mask, 'nome'].values[0]

            result.loc[mask, 'sigla'] = update_info['nova_sigla']

            current_alt = result.loc[mask, 'siglas_alt'].values[0]
            if current_alt and not pd.isna(current_alt):
                result.loc[mask, 'siglas_alt'] = f"{current_alt}|{old_sigla}"
            else:
                result.loc[mask, 'siglas_alt'] = old_sigla

            if 'novo_nome' in update_info and update_info['novo_nome']:
                result.loc[mask, 'nome'] = update_info['novo_nome']
                if old_nome != update_info['novo_nome']:
                    current_alt_nome = result.loc[mask, 'nomes_alt'].values[0]
                    if current_alt_nome and not pd.isna(current_alt_nome):
                        result.loc[mask, 'nomes_alt'] = f"{current_alt_nome}|{old_nome}"
                    else:
                        result.loc[mask, 'nomes_alt'] = old_nome

        for field in ['gs_id', 'dblp_id', 'avaliacao']:
            if field in update_info and update_info[field]:
                if pd.isna(result.loc[mask, field].values[0]):
                    result.loc[mask, field] = update_info[field]

    result = apply_unifications(result, all_unifications, rename_rules)

    print(f"\nStatistics:")
    print(f"  Total from Qualis: {len(qualis_df)}")
    print(f"  Total in CEs: {ce_total}")
    print(f"  New from CEs: {len(all_new)}")
    print(f"  Duplicates found: {duplicates_found}")
    print(f"  Unifications applied: {len(all_unifications)}")
    print(f"  Conferences unified: {sum(len(v) for v in all_unifications.values())}")

    return result

def create_conferences_csv(df):
    conferences = []
    timestamp = datetime.now().isoformat()

    for idx, row in df.iterrows():
        comp_id = row['comp_id']
        conferences.append({
            'cs_id': extract_numeric_id(comp_id),
            'name': row['nome'],
            'acronym': row['sigla'],
            'version': 1,
            'created_at': timestamp,
            'updated_at': timestamp
        })

    return pd.DataFrame(conferences)

def normalize_conference_name(name):
    if not name:
        return ""

    name = re.sub(r'https?://[^\s]+', '', name)
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r',\s*$', '', name)
    name = re.sub(r'\s+', ' ', name)
    name = name.strip()

    name = name.replace('IEEE International', 'IEEE')
    name = name.replace('ACM International', 'ACM')
    name = name.replace('International Conference', 'Conference')
    name = name.replace('Proceedings of the', '')
    name = name.replace('Proceedings', 'Conference')

    return name.strip()

def are_names_similar(name1, name2):
    if not name1 or not name2:
        return False

    n1 = normalize_conference_name(name1).lower()
    n2 = normalize_conference_name(name2).lower()

    if n1 == n2:
        return True

    if n1 in n2 or n2 in n1:
        return True

    words1 = set(n1.split())
    words2 = set(n2.split())
    common = len(words1 & words2)
    total = len(words1 | words2)

    if total > 0 and common / total > 0.7:
        return True

    return False

def is_valid_conference_name(name):
    if not name or len(name) < 3:
        return False

    if name.count('(') != name.count(')'):
        return False

    if name.endswith((')', '(', ',', '.')) and not name.endswith('Inc.'):
        if name[-1] in '(),.' and len(name) > 1:
            return is_valid_conference_name(name[:-1])

    non_name_patterns = [
        r'^Foi\s+incorporad',
        r'^Merged\s+with',
        r'^Now\s+called',
        r'^See\s+also',
        r'^Renamed\s+to',
        r'^\d{4}$',
        r'^H5\s+\d+',
        r'^Impact\s+Factor'
    ]

    for pattern in non_name_patterns:
        if re.match(pattern, name, re.IGNORECASE):
            return False

    return True

def fix_common_typos(name):
    typo_fixes = {
        'Workshp': 'Workshop',
        'Conferece': 'Conference',
        'Internation ': 'International ',
        'Compuer': 'Computer',
        'Symposim': 'Symposium',
        'Proccedings': 'Proceedings',
        'Intelligene': 'Intelligence'
    }

    for typo, fix in typo_fixes.items():
        name = name.replace(typo, fix)

    return name

def preprocess_alternative_names(names_str, base_name, sigla):
    if not names_str or pd.isna(names_str):
        return []

    corruption_indicators = [
        'RoboCup', 'Computer on The Beach', 'Workshop on Tractable',
        'International Conference of the Italian', 'International Conference on Neural',
        'https://', 'http://', '.org/', '.com/'
    ]

    names_text = str(names_str)
    for indicator in corruption_indicators:
        if indicator in names_text:
            print(f"  Warning: Skipping corrupted alternatives for {sigla}")
            return []

    names = []
    seen_normalized = set()

    for name in names_text.split('|'):
        name = name.strip()

        if not name or name.lower() == 'nan' or name == base_name:
            continue

        name = fix_common_typos(name)

        if not is_valid_conference_name(name):
            continue

        normalized = normalize_conference_name(name)
        if not normalized or len(normalized) < 5:
            continue

        norm_lower = normalized.lower()
        if norm_lower in seen_normalized:
            continue

        is_duplicate = False
        for seen_name in names:
            if are_names_similar(name, seen_name):
                is_duplicate = True
                break

        if not is_duplicate and not are_names_similar(name, base_name):
            names.append(name)
            seen_normalized.add(norm_lower)

            if len(names) >= 2:
                break

    return names

def create_additional_names_csv(df):
    additional = []
    aid = 1
    total_removed = 0
    total_original = 0

    for idx, row in df.iterrows():
        cs_id = extract_numeric_id(row['comp_id'])

        if row['siglas_alt'] and not pd.isna(row['siglas_alt']):
            original_count = len(str(row['siglas_alt']).split('|'))
            total_original += original_count

            clean_acronyms = preprocess_alternative_names(
                row['siglas_alt'],
                row['sigla'],
                row['sigla']
            )

            for alt in clean_acronyms[:2]:
                additional.append({
                    'additional_name_id': aid,
                    'cs_id': cs_id,
                    'additional_name': '',
                    'additional_acronym': alt
                })
                aid += 1

            total_removed += original_count - len(clean_acronyms)

        if row['nomes_alt'] and not pd.isna(row['nomes_alt']):
            original_count = len(str(row['nomes_alt']).split('|'))
            total_original += original_count

            clean_names = preprocess_alternative_names(
                row['nomes_alt'],
                row['nome'],
                row['sigla']
            )

            for alt in clean_names[:2]:
                additional.append({
                    'additional_name_id': aid,
                    'cs_id': cs_id,
                    'additional_name': alt,
                    'additional_acronym': ''
                })
                aid += 1

            total_removed += original_count - len(clean_names)

    if total_original > 0:
        print(f"  Alternative names: {total_original} -> {len(additional)} (removed {total_removed} duplicates/corrupted)")

    return pd.DataFrame(additional)

def create_editions_csv(df):
    editions = []
    eid = 1
    timestamp = datetime.now().isoformat()

    for idx, row in df.iterrows():
        cs_id = extract_numeric_id(row['comp_id'])

        year = None
        if row['ano_dados'] and not pd.isna(row['ano_dados']):
            try:
                year = int(float(row['ano_dados']))
            except:
                year = None

        if year and year > 2000 and year < 2030:
            editions.append({
                'edition_id': eid,
                'cs_id': cs_id,
                'name': row['nome'],
                'acronym': row['sigla'],
                'society': row['sociedade'] if row['sociedade'] and not pd.isna(row['sociedade']) else '',
                'year': year,
                'total_citations': 0,
                'total_papers': 0,
                'h5_index': None,
                'isbn': '',
                'doi': '',
                'extended_issn': '',
                'id_springer': '',
                'id_scholar_conference': row['gs_id'] if row['gs_id'] and not pd.isna(row['gs_id']) else '',
                'id_dblp_conference': row['dblp_id'] if row['dblp_id'] and not pd.isna(row['dblp_id']) else '',
                'id_openalex_proceedings': '',
                'created_at': timestamp
            })
            eid += 1

    return pd.DataFrame(editions)

def generate_quality_report(merged_df, conf_df, add_df, ed_df, config, rename_rules):
    print("\n" + "="*60)
    print("QUALITY REPORT")
    print("="*60)

    comp9_count = sum(1 for x in merged_df['comp_id'] if x.startswith('CompID9'))
    comp8_count = sum(1 for x in merged_df['comp_id'] if x.startswith('CompID8'))

    print("\n1. CompID Distribution:")
    print(f"   CompID9xxxxx (Qualis): {comp9_count}")
    print(f"   CompID8xxxxx (CEs new): {comp8_count}")

    print("\n2. Data Coverage:")
    gs_coverage = sum(1 for x in merged_df['gs_id'] if x and not pd.isna(x))
    dblp_coverage = sum(1 for x in merged_df['dblp_id'] if x and not pd.isna(x))
    top_coverage = sum(1 for x in merged_df['avaliacao'] if x and not pd.isna(x))

    print(f"   Google Scholar IDs: {gs_coverage}/{len(merged_df)} ({gs_coverage*100/len(merged_df):.1f}%)")
    print(f"   DBLP IDs: {dblp_coverage}/{len(merged_df)} ({dblp_coverage*100/len(merged_df):.1f}%)")
    print(f"   TOP classification: {top_coverage}/{len(merged_df)} ({top_coverage*100/len(merged_df):.1f}%)")

    print("\n3. Duplicates Analysis:")
    dup_df = conf_df[conf_df['acronym'].duplicated(keep=False)].sort_values('acronym')
    if len(dup_df) > 0:
        print(f"   Found {len(dup_df.groupby('acronym'))} duplicate acronyms:")
        for acronym in dup_df['acronym'].unique():
            count = len(dup_df[dup_df['acronym'] == acronym])
            print(f"     - {acronym}: {count} occurrences")
    else:
        print("   No duplicates found!")

    print("\n4. Unifications Applied:")
    unifications = rename_rules.get('unifications', {})
    for unified_name, info in unifications.items():
        if isinstance(info, dict):
            absorbed = info.get('absorbs', [])
            if absorbed:
                print(f"   {unified_name} <- {', '.join(absorbed)}")

    print("\n5. Qualis Duplicates Removed:")
    qualis_dups = rename_rules.get('qualis_duplicates', {}).get('unify', {})
    for sigla, info in qualis_dups.items():
        removed = info.get('remove', [])
        if removed:
            print(f"   {sigla}: removed {', '.join(removed)}")

    print("\n6. Data Quality Metrics:")
    empty_names = conf_df[conf_df['name'] == ''].shape[0]
    empty_acronyms = conf_df[conf_df['acronym'] == ''].shape[0]

    print(f"   Empty names: {empty_names}")
    print(f"   Empty acronyms: {empty_acronyms}")
    print(f"   Orphan additional names: {len(set(add_df['cs_id']) - set(conf_df['cs_id'])) if len(add_df) > 0 else 0}")
    print(f"   Orphan editions: {len(set(ed_df['cs_id']) - set(conf_df['cs_id'])) if len(ed_df) > 0 else 0}")

    print("\n7. Expectations Check:")
    expected_min = config['expected_results']['min_conferences']
    total_confs = len(conf_df)

    if total_confs >= expected_min and total_confs <= 1650:
        print(f"   [OK] Total conferences ({total_confs}) within expected range ({expected_min}-1650)")
    else:
        print(f"   [FAIL] Total conferences ({total_confs}) outside expected range ({expected_min}-1650)")

    print("\n" + "="*60)

def validate_data(conf_df, add_df, ed_df, config):
    errors = []
    warnings = []

    conf_ids = set(conf_df['cs_id'])
    add_ids = set(add_df['cs_id']) if len(add_df) > 0 else set()
    ed_ids = set(ed_df['cs_id']) if len(ed_df) > 0 else set()

    orphan_add = add_ids - conf_ids
    if orphan_add:
        errors.append(f"Orphan records in additional_names: {len(orphan_add)}")

    orphan_ed = ed_ids - conf_ids
    if orphan_ed:
        errors.append(f"Orphan records in editions: {len(orphan_ed)}")

    empty_names = conf_df[conf_df['name'] == ''].shape[0]
    if empty_names > 0:
        errors.append(f"Conferences with empty names: {empty_names}")

    empty_acronyms = conf_df[conf_df['acronym'] == ''].shape[0]
    if empty_acronyms > 0:
        errors.append(f"Conferences with empty acronyms: {empty_acronyms}")

    dup_acronyms = conf_df[conf_df['acronym'].duplicated()].shape[0]
    if dup_acronyms > 0:
        warnings.append(f"Duplicate acronyms: {dup_acronyms}")
        dup_list = conf_df[conf_df['acronym'].duplicated(keep=False)].sort_values('acronym')
        print("  Duplicates found:")
        for acronym in dup_list['acronym'].unique()[:10]:
            print(f"    - {acronym}")

    total_confs = len(conf_df)
    expected_min = config['expected_results']['min_conferences']
    if total_confs < expected_min:
        warnings.append(f"Total conferences ({total_confs}) below expected minimum ({expected_min})")

    if total_confs > 1650:
        warnings.append(f"Total conferences ({total_confs}) seems high, check for issues")

    return errors, warnings

def main():
    config = load_config()
    rename_rules = load_rename_rules()

    print("Step 1: Loading Qualis dataset")
    qualis_raw = load_qualis(config)
    qualis_df = process_qualis(qualis_raw, rename_rules)
    print(f"  Loaded {len(qualis_df)} conferences from Qualis (after removing duplicates)")

    print("\nStep 2: Loading CE files")
    ce_files = load_ce_files(config)
    print(f"  Found {len(ce_files)} CE files")

    print("\nStep 3: Merging datasets with rename rules")
    merged_df = merge_all(qualis_df, ce_files, config, rename_rules)
    print(f"  Total after merge: {len(merged_df)} conferences")

    print("\nStep 4: Creating normalized tables")
    conf_df = create_conferences_csv(merged_df)
    add_df = create_additional_names_csv(merged_df)
    ed_df = create_editions_csv(merged_df)

    print(f"  conferences.csv: {len(conf_df)} records")
    print(f"  conference_additional_names.csv: {len(add_df)} records")
    print(f"  conference_editions.csv: {len(ed_df)} records")

    print("\nStep 5: Validating data")
    errors, warnings = validate_data(conf_df, add_df, ed_df, config)

    if errors:
        print("  Validation ERRORS:")
        for error in errors:
            print(f"    ERROR: {error}")
        print("\n  Stopping due to validation errors")
        return

    if warnings:
        print("  Validation warnings:")
        for warning in warnings:
            print(f"    Warning: {warning}")

    print("  Validation passed (with warnings)" if warnings else "  Validation passed")

    print("\nStep 6: Saving files")
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'out')
    os.makedirs(output_dir, exist_ok=True)

    conf_df.to_csv(os.path.join(output_dir, 'conferences.csv'), index=False, encoding='utf-8')
    add_df.to_csv(os.path.join(output_dir, 'conference_additional_names.csv'), index=False, encoding='utf-8')
    ed_df.to_csv(os.path.join(output_dir, 'conference_editions.csv'), index=False, encoding='utf-8')

    merged_df.to_csv(os.path.join(output_dir, 'merged_conferences.csv'), index=False, encoding='utf-8', na_rep='')

    gs_count = sum(1 for x in merged_df['gs_id'] if x and not pd.isna(x))
    dblp_count = sum(1 for x in merged_df['dblp_id'] if x and not pd.isna(x))

    print("\nFinal Summary:")
    print(f"  Total conferences: {len(conf_df)}")
    print(f"  With GS ID: {gs_count}")
    print(f"  With DBLP ID: {dblp_count}")
    print(f"  Additional names: {len(add_df)}")
    print(f"  Editions: {len(ed_df)}")

    generate_quality_report(merged_df, conf_df, add_df, ed_df, config, rename_rules)

    print("\nDone!")

if __name__ == "__main__":
    main()