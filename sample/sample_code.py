import pandas as pd

data = [['tom', 10], ['nick', 15], ['juli', 14]]

df = pd.DataFrame(data, columns=['name', 'age'])

print(df)
df.loc[df["age"].to_numpy() == 15, "name"] = "HS"
print(df)
