import pandas as pd
from dateutil.parser import parse

# Path to input and output files
input_file = '/home/hduser/x22240217/SalesFINAL12312016.csv'
output_file = '/home/hduser/x22240217/cleaned_data.csv'

df = pd.read_csv(input_file)

# Null value check
if df.isnull().values.any():
    print("There are null values in the dataset.")
else:
    print("No null values found in the dataset")

# Handling dates in different formats and converting them into a single format to dd-mm-yyyy
def parse_date(date_str):
    try:
        # To handle date with dd/mm/yyyy format
        return parse(date_str, dayfirst=True).strftime('%d-%m-%Y')
    except ValueError:
        try:
            # To handle date with mm-dd-yyyy format
            return parse(date_str).strftime('%d-%m-%Y')
        except ValueError:
            # To handle invalid date values
            return None

print("SalesDate column has been handled")

# Applying the parsing function to the date column
df['SalesDate'] = df['SalesDate'].apply(parse_date)

# To print the DataFrame to a output csv file
df.to_csv(output_file, index=False)

print("New CSV file has been Created with Correct SalesDateValues", output_file)
print("  ")
df1 = pd.read_csv(output_file)
print("Summary of the columns of new file")
print(df1.info())
