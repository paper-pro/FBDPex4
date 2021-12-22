import pandas as pd

df = pd.read_csv('../train_data.csv')
res = df['industry']
res.to_csv('tra_industry_data.csv', index=None, header=None)
print(res.unique())
