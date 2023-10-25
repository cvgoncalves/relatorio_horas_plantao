import pandas as pd
from datetime import datetime
from helpers import get_data


today = pd.Timestamp(datetime.today()).strftime('%d-%m-%Y')

# Define the time window (2 months)
filtered_df = get_data(today)

# Filter the DataFrame
# filtered_df = df[(df['Data'] <= today) & (df['Data'] >= today - time_window)]
filtered_df = filtered_df[(filtered_df['CRM'].notnull()) & (filtered_df['CRM'] != 'valor não encontrado')]
params = filtered_df[['Nome completo', 'CRM']].drop_duplicates()

# Write the params to the params.txt file
params.to_csv('params.txt', header=False, index=False, sep=',')