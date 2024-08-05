import pandas as pd
path = r'dbtproj\dbtpuddi\seeds\spotify.csv'
df = pd.read_csv(path, encoding='ISO-8859-1')
df.to_csv(path, encoding='utf-8', index=False)