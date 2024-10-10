import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Read the CSV file into a DataFrame
df = pd.read_csv('/home/hduser/x22240217/output3.csv', names=['Factors', 'Values'])

# Extract row and column indices (factors) and their correlation values
factors = []
values = []
for row in df.itertuples(index=False):
    factor, value = row.Factors.split('\t')
    factors.append([int(i) for i in factor.split('-')])
    values.append(float(value))

# Define custom factor labels
factor_labels = ['brand', 'excise_tax', 'sales_dollars', 'sales_quantity']

# Determine the size of the matrix based on the maximum factors
n = max(max(factors, key=max))

# Create an empty matrix
matrix_data = np.zeros((n+1, n+1))

# Populate the matrix with correlation values
for factor, value in zip(factors, values):
    matrix_data[factor[0], factor[1]] = value
    matrix_data[factor[1], factor[0]] = value

# Create heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(matrix_data, annot=True, cmap="YlGnBu", fmt=".2f",
            xticklabels=factor_labels,
            yticklabels=factor_labels)
plt.title("Correlation Heatmap")
plt.xlabel("Factors")
plt.ylabel("Factors")
plt.show()
