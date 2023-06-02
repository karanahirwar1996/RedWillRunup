import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import posemails
import negemails

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('Data_Source')
analysis_sheet = gs.worksheet('Analysis')
all_record_analysis = analysis_sheet.get_all_records()
analysis_df = pd.DataFrame(all_record_analysis)

# Convert columns to appropriate data types
analysis_df["Deep Score"] = analysis_df["Deep Score"].replace("", np.nan).astype(float)
analysis_df["52wLow"] = analysis_df["52wLow"].replace("", np.nan).astype(float)
analysis_df["52wHigh"] = analysis_df["52wHigh"].replace("", np.nan).astype(float)
analysis_df["52wavg"] = round(((analysis_df["52wLow"] + analysis_df["52wHigh"]) / 2), 2)
analysis_df.rename({"Market Cap Label": "Label"}, axis=1, inplace=True)

# Convert 'mnChange' column to numeric type
analysis_df["mnChange"] = pd.to_numeric(analysis_df["mnChange"], errors='coerce')

# Filter data based on conditions
filtered_df = analysis_df.loc[(analysis_df['mnChange'] > 0) &
                              (analysis_df['lastPrice'] <= analysis_df['52wLow'] * 1.4) &
                              (analysis_df['Deep Score'] >50) &
                              (analysis_df['Match Stock'] != "")].copy()

# Sort filtered data based on "3mAvgVol" and "vol" in descending order
sorted_df = filtered_df.sort_values(by=["Deep Score", "vol","Buying-Triger"], ascending=False).reset_index(drop=True)

# Select relevant columns for positive and negative dataframes
sorted_posdf = sorted_df[['Match Stock', 'Label', 'lastPrice', 'mnChange', '52wLow', '52wavg','52wHigh',"Buying-Triger",'vol', 'Date', 'Deep Score']].copy()
#sorted_negdf = sorted_df[['Match Stock', 'Label', 'lastPrice', 'mnChange', '52wLow', '52wavg','52wHigh', '3mAvgVol', 'vol', 'Date', 'Deep Score']].copy()

# Call email functions with sorted dataframes and the original analysis dataframe
posemails.pos_email(sorted_posdf, analysis_df)
#negemails.neg_email(sorted_negdf, analysis_df)
