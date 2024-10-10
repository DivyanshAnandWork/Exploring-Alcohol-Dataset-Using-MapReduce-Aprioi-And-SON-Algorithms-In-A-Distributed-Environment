import pandas as pd
import matplotlib.pyplot as plt

# Reading the CSV file into a dataframe
df = pd.read_csv('/home/hduser/x22240217/SonOutput.csv')

# Sorting DataFrame in descending order by support value  
df_sorted = df.sort_values(by='Support', ascending=False)

# Selecting the top 10 itemsets
top_10_combinations = df_sorted.head(10)

# Printing the top 10 combinations
print("Top 10 combinations:")
print(top_10_combinations)

# Visualizing the top 10 combinations
plt.figure(figsize=(10, 6))
plt.barh(top_10_combinations['Itemset'], top_10_combinations['Support'], color='purple')
plt.xlabel('Support')
plt.ylabel('Itemset')
plt.title('Top 10 Combinations of SON Algorithm')
plt.gca().invert_yaxis()  # Invert y-axis to display the highest support at the top
plt.show()
