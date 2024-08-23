import pandas as pd

df = pd.read_feather('../assets/page-redirects.feather')
df.to_csv('../output/page-redirects.csv', index=False)