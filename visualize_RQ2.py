import pandas as pd
import matplotlib.pyplot as plt

# Define the order of months
month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

# Read the CSV file into a DataFrame
df = pd.read_csv('/home/hduser/x22240217/output2.csv', names=['Category_Month', 'Sales'])

# Remove extra characters and split Category_Month column
df['Category'] = df['Category_Month'].str.split(' ', n=1).str[0]
df['Month_Sales'] = df['Category_Month'].str.split(' ', n=1).str[1].str.strip()

# Clean up the Month column
df['Month'] = df['Month_Sales'].str.extract(r'([A-Za-z]+)')

# Extract the numeric part and convert Sales to numeric
df['Sales'] = pd.to_numeric(df['Month_Sales'].str.extract(r'(\d+)')[0])

# Convert 'Month' column to categorical type with specified order
df['Month'] = pd.Categorical(df['Month'], categories=month_order, ordered=True)

# Pivot the DataFrame
pivot_df = df.pivot_table(index='Month', columns='Category', values='Sales', aggfunc='sum')

print("DataFrame Content:")
print(df.head())
print("\n")
print("Pivot DataFrame:")
print(pivot_df)

# Visualization through graph
pivot_df.plot(kind='line', marker='o')
plt.title('Sales of Alcoholic Beverage Categories Across Months')
plt.xlabel('Month')
plt.ylabel('Sales')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.grid(True)
plt.legend(title='Category')
plt.tight_layout()
plt.show()
