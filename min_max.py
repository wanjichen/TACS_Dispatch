import pandas as pd
import math

df_CONFIG = pd.read_csv('config.tab', sep='\t')
df_DEST_GOAL = pd.read_csv('DEST_GOAL_BY_ENTITY.csv')

df_ALL_WIP = pd.read_csv(f'ALL_WIP.csv')
df_ALL_WIP['WIP'] = df_ALL_WIP['WIP'].fillna(0)

df_MOR = pd.read_csv('MOR.csv')
df_MOR.columns = df_MOR.columns.str.upper()

df_SHIFT = pd.read_csv('SHIFT_INFO.csv')

shift = df_SHIFT.sort_values(by='ww_shift')['ww_shift'].unique()
n_shift, n_1_shift, n_2_shift = shift[0], shift[1], shift[2]
remaining_qrts = math.ceil(48 + df_SHIFT.loc[0, 'start_current'] * 24 * 4)

Day_Shift = []
for hour in range(7, 19):
    for minute in range(0, 60, 15):
        Day_Shift.append('{:02d}:{:02d}'.format(hour, minute))

Night_Shift = []

for hour in range(19, 24):
    for minute in range(0, 60, 15):
        Night_Shift.append('{:02d}:{:02d}'.format(hour, minute))
for hour in range(0, 7):
    for minute in range(0, 60, 15):
        Night_Shift.append('{:02d}:{:02d}'.format(hour, minute))

actual_time = []

if n_shift[-1] == 'D':
    actual_time += Day_Shift[-remaining_qrts:] + \
        Night_Shift[:48-remaining_qrts]
else:
    actual_time += Night_Shift[-remaining_qrts:] + \
        Day_Shift[:48-remaining_qrts]


def create_template(product, dest_oper, dest_wip_oper, module):

    n_shift = df_SHIFT.sort_values(by='ww_shift')['ww_shift'].unique()[0]
    n_1_shift = df_SHIFT.sort_values(by='ww_shift')['ww_shift'].unique()[1]
    n_2_shift = df_SHIFT.sort_values(by='ww_shift')['ww_shift'].unique()[2]

    temp = df_ALL_WIP[(df_ALL_WIP['prod'] == product)].sort_values(by='seq_num')
    temp = temp.reset_index(drop=True)

    start = temp.index[temp['operation'].isin([2170,2173])].values[0]

    

    end = temp.index[temp['operation'] == dest_wip_oper].values[0]

    tpt = temp.iloc[start:end]['ct_goal'].sum()

    if dest_wip_oper == 599:
        tpt += 1

    n_shift_qrts = math.ceil(48 + df_SHIFT.loc[0, 'start_current']*24*4)

    if 48 + (tpt)*4 - n_shift_qrts <= 48:  # n_shift + n_1_shift
        n_1_shift_qrts = 48 + (tpt)*4 - n_shift_qrts
        n_2_shift_qrts = 0

    else:  # n_shift, n_1_shift, n_2_shift
        n_1_shift_qrts = 48
        n_2_shift_qrts = 48 + (tpt)*4 - n_shift_qrts - 48

    df = pd.DataFrame(columns=['SHIFT', 'TIME_SLOT',
                      'DEST_GOAL', 'DEST_WIP', 'MIN', 'MAX', 'AVAIL_WIP'])
    df['SHIFT'] = [n_shift] * n_shift_qrts + [n_1_shift] * \
        n_1_shift_qrts + [n_2_shift] * n_2_shift_qrts
    df['TIME_SLOT'] = [i for i in range(1, len(df)+1)]
    df['DEST_GOAL'] = 0
    df['DEST_GOAL'] = df['DEST_GOAL'].astype('float64')

    # Add Destination Goal By Shift&Entity

    temp_goal = df_DEST_GOAL[(df_DEST_GOAL['product'] == product)].copy()

    for idx, row in temp_goal.iterrows():

        qrts_goal = row['goal']/row['actual_tr_qrts']

        if row['shift'] == n_shift:

            qrts_start, qrts_end = row['start'], row['end']

            df.loc[(df['SHIFT'] == row['shift']) & (df['TIME_SLOT'] >= qrts_start) & (
                df['TIME_SLOT'] <= qrts_end), 'DEST_GOAL'] += qrts_goal

        elif row['shift'] == n_1_shift:
            qrts_start, qrts_end = row['start'] + \
                remaining_qrts, row['end'] + remaining_qrts

            df.loc[(df['SHIFT'] == row['shift']) & (df['TIME_SLOT'] >= qrts_start) & (
                df['TIME_SLOT'] <= qrts_end), 'DEST_GOAL'] += qrts_goal

        elif row['shift'] == n_2_shift:
            qrts_start, qrts_end = row['start'] + \
                remaining_qrts + 48, row['end'] + remaining_qrts + 48

            df.loc[(df['SHIFT'] == row['shift']) & (df['TIME_SLOT'] >= qrts_start) & (
                df['TIME_SLOT'] <= qrts_end), 'DEST_GOAL'] += qrts_goal

    # Add Destination WIP

    filt = (df_ALL_WIP['prod'] == product) & (df_ALL_WIP['operation'] == dest_wip_oper)

    try:
        df.loc[0, 'DEST_WIP'] = round(
            df_ALL_WIP.loc[filt, 'WIP'].values[0]/1000, 3)
    except:
        df.loc[0, 'DEST_WIP'] = 0

    for idx, row in df[1:].iterrows():
        df.loc[idx, 'DEST_WIP'] = df.loc[idx-1,
                                         'DEST_WIP'] - df.loc[idx-1, 'DEST_GOAL']

    # Add MIN Constraint

    for idx, row in df[:48].iterrows():
        goal, wip = df.loc[idx+tpt*4,
                           'DEST_GOAL'], df.loc[idx+tpt*4, 'DEST_WIP']
        if goal <= wip:
            df.loc[idx, 'MIN'] = 0
        else:
            df.loc[idx, 'MIN'] = abs(goal-wip)

    # Add MAX Constraint

    for idx, row in df[:48].iterrows():

        goal, wip = df.loc[idx+(tpt)*4,
                           'DEST_GOAL'], df.loc[idx+(tpt)*4, 'DEST_WIP']

        if goal <= wip:
            df.loc[idx, 'MAX'] = 0
        else:
            df.loc[idx, 'MAX'] = abs(goal-wip)

    # Add avail WIP (current+incoming)

    incoming_WIP = {}

    try:
        seq_start = df_ALL_WIP.loc[(df_ALL_WIP['prod'] == product) & (df_ALL_WIP['operation'] == 2044), 'seq_num'].values[0]
    except:
        seq_start = df_ALL_WIP.loc[(df_ALL_WIP['prod'] == product) & (df_ALL_WIP['operation'] == 589), 'seq_num'].values[0]
    try:
        seq_end = df_ALL_WIP.loc[(df_ALL_WIP['prod'] == product) & (df_ALL_WIP['operation'] == 2170), 'seq_num'].values[0]
    except:
        seq_end = df_ALL_WIP.loc[(df_ALL_WIP['prod'] == product) & (df_ALL_WIP['operation'] == 2173), 'seq_num'].values[0]

    temp = df_ALL_WIP[(df_ALL_WIP['prod'] == product) & (df_ALL_WIP['seq_num'] < seq_end)].sort_values(by='seq_num')
    temp = temp.reset_index(drop=True)

    for idx, row in temp.iterrows():
        if row['WIP'] > 0:
            t = temp[temp['seq_num'] >= row['seq_num']]['ct_goal'].sum()
            incoming_WIP[t] = round(row['WIP']/1000, 3)

    current_wip = round(df_ALL_WIP[(df_ALL_WIP['prod'] == product) & (
        df_ALL_WIP['operation'].isin([2170, 2173]))]['WIP'].values[0]/1000, 3)

    for idx, row in df.iterrows():
        if row['TIME_SLOT'] <= 4:
            df.loc[idx, 'AVAIL_WIP'] = current_wip
        elif ((row['TIME_SLOT']-1)//4) in incoming_WIP.keys():
            df.loc[idx, 'AVAIL_WIP'] = current_wip + \
                incoming_WIP[((row['TIME_SLOT']-1)//4)]
        else:
            df.loc[idx, 'AVAIL_WIP'] = 0

    for idx, row in df[1:].iterrows():
        if row['AVAIL_WIP'] == 0:
            df.loc[idx, 'AVAIL_WIP'] = df.loc[idx - 1, 'AVAIL_WIP']

    inf_time = df_ALL_WIP[(df_ALL_WIP['prod'] == product)& (
        df_ALL_WIP['seq_num'] > seq_start) & (df_ALL_WIP['seq_num'] < seq_end)].sort_values(by='seq_num')['ct_goal'].sum()

    df.loc[(df['TIME_SLOT']-1)//4 >= inf_time, 'AVAIL_WIP'] = 1000000

    temp = df_ALL_WIP[(df_ALL_WIP['prod'] == product)].sort_values(by='seq_num')
    temp = temp.reset_index(drop=True)

    mor = df_MOR[(df_MOR['PRODGROUP3'] == product) & (
        df_MOR['CEID'].str.contains('ACL'))]['MOR'].values[0]

    df['MOR'] = mor
    for idx, row in df.iterrows():
        df.loc[idx, ['MIN_after_avail_wip', 'MAX_after_avail_wip']] = min(
            row['MIN'], row['AVAIL_WIP']), min(row['MAX'], row['AVAIL_WIP'])

    df['OUTPUT'] = 0

    df = df[:48]
    df['ACTUAL_TIME'] = actual_time
    df.to_csv(f'{product}_TO_{module}.csv', index=False)


# In[5]:


for p in df_DEST_GOAL['product'].unique():

    dest_oper = df_CONFIG.loc[df_CONFIG['PRODUCT'] == p, 'DEST_OPER'].values[0]
    dest_wip_oper = df_CONFIG.loc[df_CONFIG['PRODUCT']
                                  == p, 'DEST_WIP_OPER'].values[0]
    module = df_CONFIG.loc[df_CONFIG['PRODUCT'] == p, 'TYPE'].values[0][-3:]
    create_template(p, dest_oper, dest_wip_oper, module)
