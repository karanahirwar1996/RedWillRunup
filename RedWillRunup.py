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
analysis_sheet=gs.worksheet('Analysis')
all_record_analysis=analysis_sheet.get_all_records()
analysis_df=pd.DataFrame(all_record_analysis)
analysis_df["Deep Score"]=analysis_df["Deep Score"].replace("",np.nan).astype(float)
analysis_df["52wLow"]=analysis_df["52wLow"].replace("",np.nan).astype(float)
analysis_df["52wHigh"]=analysis_df["52wHigh"].replace("",np.nan).astype(float)
analysis_df["52wavg"]=round(((analysis_df["52wLow"]+analysis_df["52wHigh"])/2),2)
analysis_df.rename({"Market Cap Label":"Label"},axis=1,inplace=True)
sorted_pos=analysis_df.loc[(analysis_df['Deep Score']>50)*(analysis_df['Normal Score']>=0)*(analysis_df['Match Stock']!="")].reset_index(drop=True)
sorted_neg=analysis_df.loc[(analysis_df['Deep Score']<=0)*(analysis_df['Normal Score']<=0)*(analysis_df['Match Stock']!="")].reset_index(drop=True)
sorted_posdf=sorted_pos[['Match Stock','Label','lastPrice',"mnChange","52wLow","52wavg","52wHigh", 'Date', 'Time','Deep Score']].copy()
sorted_negdf=sorted_neg[['Match Stock','Label','lastPrice',"mnChange","52wLow","52wavg","52wHigh", 'Date', 'Time','Deep Score']].copy()
posemails.pos_email(sorted_posdf,analysis_df)
negemails.neg_email(sorted_negdf,analysis_df)
