import pandas as pd





orders = pd.read_table('http://bit.ly/chiporders')
print(orders.head())
print(type(orders))
orders.ord