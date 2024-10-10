import pandas as pd
import matplotlib.pyplot as plt

# Reading the CSV file into a dataframe
df = pd.read_csv('/home/hduser/x22240217/aprioriOutput.csv')

# Sorting DataFrame in descending order by support value and selecting the top 10 itemsets
top_10 = df.nlargest(10, 'Support')

# Printing the top 10 combinations
print("Top 10 combinations:")
print(top_10)

# Visualizing the top 10 combinations
plt.figure(figsize=(10, 6))
plt.barh(top_10['Itemset'], top_10['Support'], color='orange')
plt.xlabel('Support')
plt.ylabel('Itemset')
plt.title('Top 10 Combinations of Apriori Algorithm')
plt.gca().invert_yaxis()  # Invert y-axis to display the combination with the highest support at the top
plt.show()
