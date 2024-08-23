import csv
from rapidfuzz import process

with open('../assets/page-info.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    titles = {row['id']: row['title'] for row in reader}

def search(query):
    best_match = process.extractOne(query, titles.values())
    return best_match

query = input("Digite o título que deseja buscar: ")
result = search(query)
print(result)
