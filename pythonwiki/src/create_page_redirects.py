import csv
import ast

id_title_map = {}
with open('../assets/page-info.csv', mode='r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        id_title_map[row['title'].upper()] = row['id']

with open('../assets/page-redirects.csv', mode='r', encoding='utf-8') as f, open('../output/page-redirects.csv', mode='a', newline='', encoding='utf-8') as output:
    reader = csv.DictReader(f)
    writer = csv.writer(output)
    
    writer.writerow(['source', 'target'])
    
    for row in reader:
        try:
            target_names = ast.literal_eval(row['Target Page Name'])
            
            for name in target_names:
                uppercase_name = name.upper()
                if uppercase_name in id_title_map:
                    source_id = row['Source Page ID']
                    target_id = id_title_map[uppercase_name]
                    print(f"source: {source_id}, target: {target_id}")
                    writer.writerow([source_id, target_id])
        except (ValueError, SyntaxError) as e:
            print(f"Erro ao processar a linha com Source Page ID {row['Source Page ID']}: {e}")
            continue
