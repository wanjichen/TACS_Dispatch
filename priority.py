from datetime import datetime, timedelta, date
import pandas as pd
import os

PRODUCT = []

file_list = [f for f in os.listdir() if '_TO_' in f]

for x in file_list:
    split_str = x.split('_')
    PRODUCT.append(split_str[0])

PRODUCT.sort()


df = pd.DataFrame(
    columns=['product', 'min_first_occur', 'goal_first_occur', 'max_output'])


for p in PRODUCT:

    matched_file = [f for f in file_list if p in f][0]
    df_prod = pd.read_csv(matched_file)

    mor = round(df_prod['MOR'].values[0], 2)

    try:
        min_first_occur = df_prod[df_prod['MIN'] > 0].index[0]
    except:
        min_first_occur = 48
    try:
        goal_first_occur = df_prod[df_prod['DEST_GOAL'] > 0].index[0]
    except:
        goal_first_occur = 48

    try:
        max_output = round(mor*df_prod[df_prod['MAX'] > 0]['MAX'].values[-1]/4/12, 2)
    except:
        max_output = 0

    df.loc[len(df)] = p, min_first_occur, goal_first_occur, max_output
    
df.sort_values(
    by=['min_first_occur', 'goal_first_occur', 'max_output'], ascending=[True, True, False], inplace=True)
df['max_output'] = round(df['max_output'],3)
df.to_csv('TACS_priority.csv', index=False)
