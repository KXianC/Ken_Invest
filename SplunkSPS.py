import pandas as pd
import os
import time
from datetime import datetime

##################################################################
def Read_csv_file(path):#Control the data volume for test

    df = pd.DataFrame.from_csv(path)
    df2 = df[0:]
    # print(df.head())
    df2.to_csv("1704_splunk_raw_300.csv")
# Read_csv_file("/Users/xian.chen/Documents/KenPro/SPS/1704_splunk_raw.csv")

def dummy_delete():
    df3 = pd.DataFrame.from_csv("/Users/xian.chen/Documents/KenPro/SPS/1704_splunk_raw.csv")
    # print(df3['dummyFlg'])
    # print(df3.dummyFlg)
    # for each_record in df3['dummyFlg']:
    #     if each_record = 1
    df3 = df3[df3.dummyFlg != 1]
    return df3
    # print(df3)

# for all splunk data, after delete dummy row, the output is df3



def read_genre_Id_file():
    genre_Id_df = pd.read_csv("SPS/genrelist0614.tsv", sep='	',low_memory=False)
    # print(genre_Id_df)
    return genre_Id_df

def read_easyId_file():
    easy_Id_df = pd.DataFrame.from_csv("SPS/easy_id_info.csv")
    easy_Id_df = easy_Id_df.reset_index()
    # print(genre_Id_df)
    return easy_Id_df

def read_final_result():
    result_df = pd.read_csv("SPS/1704_sps_return.txt", sep='	',low_memory=False)
    result_df.columns = ["shop_id", "item_id", "genre_id", "result","deleted_flg", "notcheck"]


#                            , names=["shop_id", "item_id", "genre_id", "result","deleted_flg", "notcheck"])
    # result_df1 = pd.DataFrame([result_df], columns=["shop_id", "item_id", "genre_id", "result","deleted_flg", "notcheck"])
    # print(result_df)
    return result_df


df_sps = dummy_delete()
# print(df_sps)
df_genre = read_genre_Id_file()
# print(df_sps.head())
# print(df_genre.head())
df_gender = read_easyId_file()
# print(df_gender)
result_df = read_final_result()


#
df_combine1 = pd.merge(df_sps, df_genre, left_on='currenGenreId', right_on='genre_id', how='left')
# print(df_combine1.head(3))
df_combine2 = pd.merge(df_combine1, df_gender, left_on='easy_id', right_on='easy_id', how='left')
df_combine3 = pd.merge(df_combine2, result_df, left_on=['shopId','itemId'], right_on=['shop_id','item_id'], how='left')
df_combine3['accept_flg'] = 0


#drop columns
# df = df.drop('column_name', 1)
df_combine4 = df_combine3.drop(df_combine3.columns[[1, 4,5,6,8,11,12,13,14,16,17,18,20,21]], axis=1)

# df_combine4 = df_combine4[df_combine4.g1 != 'NaN']
df_combine4 = df_combine4[pd.notnull(df_combine4.g1)]


# df_combine4.result[df_combine3.result == 'Y'] = 1
# df_combine4.result[df_combine3.result == 'N'] = 2
# df_combine4.result[df_combine3.result == 'U'] = 3
# df_combine4 = df_combine3 + df_accept
# print(df_combine3)
# print(result_df)

# for flg in df_combine4.accept_flg:
#     if df_combine4.userResult == df_combine4.result:
#         df_combine4.accept_flg = 1
#     else:
#         df_combine4.accept_flg = 0

# df_combine4[ (df_combine4.userResult==df_combine4.result) ][df_combine4.accept_flg] = 1
# df_combine4[ (df_combine4.userResult !=df_combine4.result) ][df_combine4.accept_flg] = 0

# df_combine4.loc[df_combine4.userResult == df_combine4.result, 'accept_flg'] =1
df_combine4.loc[(df_combine4.userResult == 1) & (df_combine4.result == 'Y'), 'accept_flg'] =1
df_combine4.loc[(df_combine4.userResult == 2) & (df_combine4.result == 'N'), 'accept_flg'] =1
df_combine4.loc[(df_combine4.userResult == 3) & (df_combine4.result == 'U'), 'accept_flg'] =1

# print(df_combine4.head())

# df_combine4.head(10000).to_csv("test.csv")

#Create the final data for plotting


#Read L1 LIst
# L1_list = pd.DataFrame.from_csv("SPS/L1_list.csv")
L1_list = pd.read_csv("SPS/L1_list.csv")
# print(L1_list)
# g1_list = L1_list['g1']
# gn1_list = L1_list['gn1']
# print(g1_list)

df_final = pd.DataFrame(columns=[
                        'L1_genre',
                        # 'L1_genre_name',
                        'total_accept',
                        'total_all',
                        'total_rate',
                        'male_accept',
                        'male_all',
                        'male_rate',
                        'female_accept',
                        'female_all',
                        'female_rate'
                           ])

for genre in L1_list['g1']:
    L1_genre = genre
    # L1_genre_name = L1_list[L1_list['g1']==genre]['gn1']
    # print(L1_genre_name)
    try:
        total_accept = df_combine4[(df_combine4.accept_flg == 1) & (df_combine4.g1 == genre)].count()['accept_flg']
        total_all = df_combine4[(df_combine4.g1 == genre)].count()['accept_flg']
        total_rate = total_accept / total_all

        male_accept = df_combine4[(df_combine4.accept_flg == 1) & (df_combine4.gender_cd == 1)& (df_combine4.g1 == genre) ].count()['accept_flg']
        male_all = df_combine4[(df_combine4.gender_cd == 1)& (df_combine4.g1 == genre)].count()['accept_flg']
        male_rate = male_accept / male_all

        female_accept = df_combine4[(df_combine4.accept_flg == 1) & (df_combine4.gender_cd == 2) & (df_combine4.g1 == genre)].count()['accept_flg']
        female_all = df_combine4[(df_combine4.gender_cd == 2)& (df_combine4.g1 == genre)].count()['accept_flg']
        female_rate = female_accept / female_all
    except Exception as e:
        pass

    df_final = df_final.append({
                                'L1_genre':genre,
                                # 'L1_genre_name':L1_genre_name,
                                'total_accept':total_accept,
                                'total_all':total_all,
                                'total_rate':total_rate,
                                'male_accept':male_accept,
                                'male_all':male_all,
                                'male_rate':male_rate,
                                'female_accept':female_accept,
                                'female_all':female_all,
                                'female_rate':female_rate
                                }, ignore_index=True)


print(df_final)
df_final.to_csv("ttttt.csv")



