import pandas as pd
import os
import time
from datetime import datetime

def read_final_result():
    result_df = pd.read_csv("SPS/1704_sps_return.txt", sep='	',low_memory=False)
    result_df.columns = ["shop_id", "item_id", "genre_id", "result","deleted_flg", "notcheck"]
    result_df = result_df[result_df.result == 'Y']
    result_df = result_df.head()
    # print(result_df)

    # df3 = pd.DataFrame.from_csv("splunkraw10000.csv")#"1704_splunk_raw_300.csv")
    df3 = pd.DataFrame.from_csv("/Users/xian.chen/Documents/KenPro/SPS/1704_splunk_raw.csv")


    # df3 = pd.DataFrame.from_csv("1704_splunk_raw_300.csv")
    # df3.head(10000).to_csv("splunkraw10000.csv")
    df3 = df3[df3.dummyFlg != 1]
    # df3[df3.userResult == 1].userResult = 999
    df3.loc[(df3.userResult == 2),'userResult'] = -999
    df3.loc[(df3.userResult == 3),'userResult'] = -9999
    # print(df3[df3.userResult == 2])
    # print(df3.head())
    df4 = df3.groupby(['shopId','itemId']).userResult.sum()
    df4 = df4[df4 >0]
    df_dummy_list = df4.to_frame().reset_index()
    print(df_dummy_list.sort_values(by = 'userResult', ascending=False).head(1000))
    df_dummy_list.sort_values(by = 'userResult', ascending=False).head(1000).to_csv("DummyYes1000.csv")
    # df_dummy_list.sort_values(by='userResult', ascending=False).head(1000).to_csv("DummyNo1000.csv")

    # df3 = df3.groupby(['shopId', 'itemId'])
    # print(df3)
    # df_combine = pd.merge(result_df, df_dummy_list, left_on=['shop_id', 'item_id'], right_on=['shopId', 'itemId'], how='left')
    # print(df_combine.head())


    # for shopid in df_combine.shop_id:
    #     if ((df_combine[df_combine.shop_id == shopid].result =='Y') & (df_combine[df_combine.shop_id == shopid].userResult == 1)):
    #         df_combine = df_combine[df_combine.shop_id != shopid]
    #     else:
    #         pass

    # df_combine.to_csv("DummyNo.csv")
    # df_combine.head(5000).to_csv("DummyNo5000.csv")

    # for [shop_id,item_id] in result_df['shop_id','item_id']:
    #     # countY = df3[(df3.shopId == shop_id) & (df3.itemId == item_id)].count()
    #     # countY = df3[df3['shopId','itemId'] == [shop_id,item_id]].count()['']
    #     # print(str(shop_id+item_id))
    #     # print(countY)
    #     pass


# & (str(df3.itemId) == str(result_df.item_id))
read_final_result()