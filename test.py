import pandas as pd

# Load the new CSV file
new_csv_file_path = 'data/etrade-dev.csv'
new_data = pd.read_csv(new_csv_file_path)

# Display the first few rows of the dataframe
print(new_data.head())
print(new_data)
