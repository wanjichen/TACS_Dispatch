import pandas as pd
import numpy as np
import math


df_CONFIG = pd.read_csv('config.tab', sep='\t')
df_DEST_LOADING_PLAN = pd.read_csv('DEST_LOADING_PLAN.csv')
df_SHIFT = pd.read_csv('SHIFT_INFO.csv')


shift = df_SHIFT.sort_values(by='ww_shift')['ww_shift'].unique()
n_shift, n_1_shift, n_2_shift = shift[0], shift[1], shift[2]


remaining_qrts = math.ceil(48 + df_SHIFT.loc[0, 'start_current'] * 24 * 4)
df_result = pd.DataFrame(columns=['shift', 'entity', 'product', 'flow_name', 'operation',
                         'tr_hrs', 'tr_qrts', 'remaining_qrts', 'actual_tr_qrts', 'mor', 'start', 'end'])

df_PLAN = df_DEST_LOADING_PLAN.merge(df_CONFIG, how='left', left_on=[
                                     'prodgroup3', 'operation'], right_on=['PRODUCT', 'DEST_OPER'])

df_PLAN.to_csv('JOINED_PLAN.csv', index=False)

df_SHIFT_ENTITY = df_PLAN[df_PLAN['TYPE'].notnull(
)][['ww_shift', 'entity', 'operation']].sort_values(by=['ww_shift', 'entity', 'operation'])
df_SHIFT_ENTITY = df_SHIFT_ENTITY.drop_duplicates(ignore_index=True)

for idx, row in df_SHIFT_ENTITY.iterrows():

    temp = df_PLAN[(df_PLAN['ww_shift'] == row['ww_shift'])
                   & (df_PLAN['entity'] == row['entity'])]

    temp = temp.sort_values(by='seq')

    rem_qrts = remaining_qrts if row['ww_shift'] == n_shift else 48

    seq_list = list(temp['seq'].unique())
    seq_list.sort()

    # only seq1

    if len(seq_list) == 1:
        prod = temp['PRODUCT'].values[0]
        tr_hrs = round(temp['tr_hrs'].values[0]/1000, 2)
        tr_qrts = math.ceil(tr_hrs * 4)
        mor = temp['mor'].values[0]

        # special case: put 100 as goal -> arrange from shift end
        if temp['goal'].values[0] == 100:
            start, end = rem_qrts-1, rem_qrts

        else:
            actual_tr_qrts = min(rem_qrts, tr_qrts)
            start, end = 1, actual_tr_qrts

        flow_name = temp['flow_name'].values[0]

        df_result.loc[len(df_result)] = [row['ww_shift'], row['entity'], prod, flow_name,
                                         row['operation'], tr_hrs, tr_qrts, rem_qrts, actual_tr_qrts, mor, start, end]

    # seq1 + seq2

    elif len(seq_list) == 2:

        # seq1
        tr_hrs_seq1 = round(
            temp.loc[temp['seq'] == seq_list[0], 'tr_hrs'].values[0]/1000, 3)
        tr_qrts_seq1 = math.ceil(tr_hrs_seq1 * 4)

        if len(temp[(temp['seq'] == seq_list[0]) & (temp['PRODUCT'].notnull())]) > 0:

            prod_seq1 = temp.loc[temp['seq'] ==
                                 seq_list[0], 'PRODUCT'].values[0]
            mor_seq1 = temp.loc[temp['seq'] == seq_list[0], 'mor'].values[0]

            actual_tr_qrts_seq1 = min(rem_qrts, tr_qrts_seq1)
            start_seq1, end_seq1 = 1, actual_tr_qrts_seq1

            flow_name_seq1 = temp.loc[temp['seq'] ==
                                      seq_list[0], 'flow_name'].values[0]

            df_result.loc[len(df_result)] = [row['ww_shift'], row['entity'], prod_seq1, flow_name_seq1, row['operation'],
                                             tr_hrs_seq1, tr_qrts_seq1, rem_qrts, actual_tr_qrts_seq1, mor_seq1, start_seq1, end_seq1]

        # seq2
        if tr_qrts_seq1 < remaining_qrts:

            tr_hrs_seq2 = round(temp.loc[temp['seq'] ==
                                         seq_list[1], 'tr_hrs'].values[0]/1000, 3)

            tr_qrts_seq2 = math.ceil(tr_hrs_seq2 * 4)

            if len(temp[(temp['seq'] == seq_list[1]) & (temp['PRODUCT'].notnull())]) > 0:

                prod_seq2 = temp.loc[temp['seq'] ==
                                     seq_list[1], 'PRODUCT'].values[0]
                mor_seq2 = temp.loc[temp['seq'] ==
                                    seq_list[1], 'mor'].values[0]

                actual_tr_qrts_seq2 = min(
                    rem_qrts - tr_qrts_seq1, tr_qrts_seq2)

                if tr_qrts_seq2 >= rem_qrts - tr_qrts_seq1:
                    start_seq2, end_seq2 = tr_qrts_seq1 + 1, rem_qrts

                else:
                    start_seq2, end_seq2 = rem_qrts-tr_qrts_seq2, rem_qrts

                flow_name_seq2 = temp.loc[temp['seq']
                                          == seq_list[1], 'flow_name'].values[0]

                df_result.loc[len(df_result)] = [row['ww_shift'], row['entity'], prod_seq2, flow_name_seq2, row['operation'],
                                                 tr_hrs_seq2, tr_qrts_seq2, rem_qrts, actual_tr_qrts_seq2, mor_seq2, start_seq2, end_seq2]

    # seq1 + seq2 + seq3

    elif len(seq_list) == 3:

        # seq1
        tr_hrs_seq1 = round(
            temp.loc[temp['seq'] == seq_list[0], 'tr_hrs'].values[0]/1000, 3)
        tr_qrts_seq1 = math.ceil(tr_hrs_seq1 * 4)

        if len(temp[(temp['seq'] == 1) & (temp['PRODUCT'].notnull())]) > 0:

            prod_seq1 = temp.loc[temp['seq'] ==
                                 seq_list[0], 'PRODUCT'].values[0]
            mor_seq1 = temp.loc[temp['seq'] == seq_list[0], 'mor'].values[0]

            actual_tr_qrts_seq1 = min(rem_qrts, tr_qrts_seq1)
            start_seq1, end_seq1 = 1, actual_tr_qrts_seq1

            flow_name_seq1 = temp.loc[temp['seq'] ==
                                      seq_list[0], 'flow_name'].values[0]

            df_result.loc[len(df_result)] = [row['ww_shift'], row['entity'], prod_seq1, flow_name_seq1, row['operation'],
                                             tr_hrs_seq1, tr_qrts_seq1, rem_qrts, actual_tr_qrts_seq1, mor_seq1, start_seq1, end_seq1]

        # seq2
        if tr_qrts_seq1 < rem_qrts:

            tr_hrs_seq2 = round(temp.loc[temp['seq'] ==
                                         seq_list[1], 'tr_hrs'].values[0]/1000, 3)
            tr_qrts_seq2 = math.ceil(tr_hrs_seq2 * 4)

            if len(temp[(temp['seq'] == seq_list[1]) & (temp['PRODUCT'].notnull())]) > 0:

                prod_seq2 = temp.loc[temp['seq'] ==
                                     seq_list[1], 'PRODUCT'].values[0]
                mor_seq2 = temp.loc[temp['seq'] ==
                                    seq_list[1], 'mor'].values[0]

                actual_tr_qrts_seq2 = min(rem_qrts - tr_qrts_seq1, rem_qrts)

                if tr_qrts_seq2 >= rem_qrts - tr_qrts_seq1:
                    start_seq2, end_seq2 = tr_qrts_seq1 + 1, rem_qrts
                else:
                    start_seq2 = tr_qrts_seq1 + 1
                    end_seq2 = start_seq2 + tr_qrts_seq2 - 1

                flow_name_seq2 = temp.loc[temp['seq']
                                          == seq_list[1], 'flow_name'].values[0]

                df_result.loc[len(df_result)] = [row['ww_shift'], row['entity'], prod_seq2, flow_name_seq2, row['operation'],
                                                 tr_hrs_seq2, tr_qrts_seq2, rem_qrts, actual_tr_qrts_seq2, mor_seq2, start_seq2, end_seq2]

        # seq3
        if (tr_qrts_seq1 + tr_qrts_seq2) < rem_qrts:

            tr_hrs_seq3 = round(temp.loc[temp['seq'] ==
                                         seq_list[2], 'tr_hrs'].values[0]/1000, 3)
            tr_qrts_seq3 = math.ceil(tr_hrs_seq3 * 4)

            if len(temp[(temp['seq'] == seq_list[2]) & (temp['PRODUCT'].notnull())]) > 0:

                prod_seq3 = temp.loc[temp['seq'] ==
                                     seq_list[2], 'PRODUCT'].values[0]
                mor_seq3 = temp.loc[temp['seq'] ==
                                    seq_list[2], 'mor'].values[0]

                if tr_qrts_seq3 >= rem_qrts - tr_qrts_seq1 - tr_qrts_seq2:
                    actual_tr_qrts_seq3 = rem_qrts - tr_qrts_seq1 - tr_qrts_seq2
                    start_seq3, end_seq3 = tr_qrts_seq1 + tr_qrts_seq2 + 1, rem_qrts
                else:
                    actual_tr_qrts_seq3 = tr_qrts_seq3
                    start_seq3 = rem_qrts - tr_qrts_seq3
                    end_seq3 = rem_qrts

                flow_name_seq3 = temp.loc[temp['seq']
                                          == seq_list[2], 'flow_name'].values[0]

                df_result.loc[len(df_result)] = [row['ww_shift'], row['entity'], prod_seq3, flow_name_seq3, row['operation'],
                                                 tr_hrs_seq3, tr_qrts_seq3, rem_qrts, actual_tr_qrts_seq3, mor_seq3, start_seq3, end_seq3]


# In[10]:


df_result = df_result[df_result['tr_hrs'] != 0]


# In[12]:

for idx, row in df_result.iterrows():
    if row['tr_qrts'] >= row['remaining_qrts']:
        df_result.loc[idx, 'goal'] = row['mor'] * row['actual_tr_qrts']/48
    else:
        df_result.loc[idx, 'goal'] = row['mor'] * row['tr_hrs']/12

df_result['goal'] = round(df_result['goal'], 3)


# In[13]:

df_result = df_result[df_result['actual_tr_qrts'] > 0]
df_result.to_csv('DEST_GOAL_BY_ENTITY.csv', index=False)
