# Author: JLLLLL 2016.01.09

import pandas as pd
import numpy as np
import glob
import os


def change(today_position, change_files, changed_file):
    # today_position = "ms20170110_pos_2.xls"
    # changed_file = "changed.xlsx"

    # Read excel of daily position
    df = pd.read_excel(today_position, sheetname=None, header=1,
                       parse_cols=None, index_col=None, converters={"stockcode": str})
    for i in df.keys():
        df[i].set_index("stockcode", inplace=True)

    # f is the list of change position while file_list is the change_file list
    f = glob.glob("./" + change_files + "/*.csv")
    file_dict = {}
    f = map(lambda x: unicode(os.path.splitext(os.path.basename(x))[0], "gbk"), f)
    for file_name in f:
        file_dict[file_name] = pd.read_csv(
            "./" + change_files + "/" + file_name + ".csv", header=0, encoding="gbk", converters={"coid": str})
        file_dict[file_name]["coid"] = map(
            lambda x: "0" * (6 - len(x)) + x, file_dict[file_name]["coid"])
        file_dict[file_name].rename(columns={"coid":"stockcode","trade":"available_num"}, inplace=True)
        file_dict[file_name].set_index("stockcode", inplace=True)


    for file_name in f:
        if file_name in df:
            file_dict[file_name] = file_dict[file_name].loc[file_dict[file_name]["available_num"] < 0]
            file_dict[file_name] = file_dict[file_name][["available_num","stockhand"]]
            file_dict[file_name]["available_num"] *= 100
            file_dict[file_name]["available_num"] = map(lambda x:int(x), file_dict[file_name]["available_num"])
            
            tmp = df[file_name]["available_num"].add(file_dict[file_name]["available_num"], fill_value=0)
            miss_stock = [x for x in tmp[tmp<0].index]
            tmp = tmp.drop(miss_stock)
            for stockcode in miss_stock:
                print "Warning: No such a stock " + stockcode + " in this account " + file_name
            error_stockhand = [x for x in tmp.index if x in file_dict[file_name].index and abs(tmp[x]-file_dict[file_name].ix[x, "stockhand"]) >= 100]
            for stockcode in error_stockhand:
                print "Error: The stockhand and available_num of the stock " + stockcode + " are not equal in the account " + file_name
            tmp = tmp.drop(error_stockhand)
            df[file_name].ix[tmp.index,"available_num"] = tmp
            
            df[file_name].reset_index(inplace=True)
            df[file_name].set_index("accountname", inplace=True)
            col = df[file_name]["stockname"]
            df[file_name].drop(labels=["stockname"], axis=1, inplace=True)
            df[file_name].insert(0, "stockname", col)

        



    writer = pd.ExcelWriter(changed_file)
    for file_name in df.keys():
        # df[sheets[i]]=df[sheets[i]].fillna(0)
        df[file_name].reset_index(inplace=True)
        df[file_name].set_index("accountname", inplace=True)
        col = df[file_name]["stockname"]
        df[file_name].drop(labels=["stockname"], axis=1, inplace=True)
        df[file_name].insert(0, "stockname", col)
        df[file_name].to_excel(writer, sheet_name=file_name)
    writer.close()


def sum(yesterday_file_final, changed_file, summed_file):
    # yesterday_file_final = "sum_20170109.xlsx"
    # changed_file = "changed.xlsx"
    # summed_file = "summed.xlsx"

    today_df = pd.read_excel(changed_file, sheetname=None, header=0,
                             parse_cols=None, index_col=None, converters={"stockcode": str})

    yesterday_df = pd.read_excel(yesterday_file_final, sheetname=0, header=0,
                                 parse_cols=None, index_col=None, converters={"stockcode": str})

    sheets = today_df.keys()

    today_df = {key: today_df[key].set_index(
        ["stockname", "stockcode"]) for key in today_df}

    # acc: list of account names
    acc = []
    for key in today_df.keys():
        tmp = pd.read_excel(changed_file, sheetname=key,
                            header=0, parse_cols=0)
        acc.append(tmp.ix[0, "accountname"])
        # today_df[key].set_index(["stockname","stockcode"], inplace = True)
        # Add from_ columns to indicate the compositon of the available_num
        today_df[key]["from_" + tmp.ix[0, "accountname"]
                      ] = today_df[key]["available_num"]
        today_df[key].drop(labels=["accountname"], axis=1, inplace=True)

    # res: the final result
    # Add all the sheets together
    res = today_df[sheets[0]]
    for i in xrange(1, len(sheets)):
        res = res.add(today_df[sheets[i]], fill_value=0)
    res = res.fillna(0)

    # Sort all the account by the sum of stocks
    col_sorted = res.columns.tolist()
    col_sorted.sort(key=lambda x: res.sum()[x], reverse=1)
    res = res[col_sorted]

    res.sort_index(level=1, inplace=True)

    
    # Delete the duplicate stock
    tmp = res.reset_index().copy()
    tmp = tmp[["stockname","stockcode"]].groupby("stockcode").first()
    res = res.groupby(level=[1]).sum()
    res = pd.concat([res, tmp], axis=1, join="outer")
    res.reset_index(inplace=True)
    res.set_index(["stockname","stockcode"], inplace=True)


    # Delete the duplicate stock
    # row_num = 0
    # pre_index = 0
    # del_list = []
    # for this_index, row in res.iterrows():
    #     # print this_index
    #     if pre_index == this_index[1]:
    #         for i in range(len(res.columns)):
    #             res.iloc[row_num - 1, i] += res.iloc[row_num, i]
    #         del_list.append(row_num)
    #     row_num = row_num + 1
    #     pre_index = this_index[1]
    # res.drop(res.index[del_list], inplace=True)

    # Join together
    yesterday_df.set_index(["stockname", "stockcode"], inplace=True)
    # print yesterday_df
    for col in yesterday_df.columns:
        if col in ['stockname', 'available_num', 'unalocated_num'] or col[0:4] == 'from':
            yesterday_df.drop(col, axis=1, inplace=True)
    res = pd.concat([res, yesterday_df], axis=1, join='outer')

    res = res.fillna(0)

    res.sort_index(level=1, inplace=True)

    res["unallocated_num"] = res["available_num"] - res["allocated_num"]
    col = res["unallocated_num"]
    res.drop(labels=["unallocated_num"], axis=1, inplace=True)
    res.insert(1, "unallocated_num", col)
    col = res["allocated_num"]
    res.drop(labels=["allocated_num"], axis=1, inplace=True)
    res.insert(1, "allocated_num", col)

    res.to_excel(summed_file)


def divide(summed_file, divided_file):
    # summed_file = "summed.xlsx"
    # divided_file = "divided.xlsx"

    df = pd.read_excel(summed_file, sheetname=0, header=0,
                       parse_cols=None, index_col=[0, 1])

    # acc: the accounts' columns
    acc = []
    for col in df.columns:
        if col.startswith("from"):
            acc.append(col)

    user = pd.read_excel(summed_file, sheetname=0, header=0, parse_cols=[
                         0, 1] + range(5 + len(acc), len(df.columns) + 2), index_col=[0, 1])

    # acc2: the final excel of sheets
    acc2 = []
    for i in range(len(acc)):
        acc2.append(pd.read_excel(summed_file, sheetname=0, header=0, parse_cols=[
                    0, 1] + [i + 5] + range(5 + len(acc), len(df.columns) + 2), index_col=[0, 1]))
        for col in acc2[i].columns:
            if col[0:4] != "from":
                acc2[i][col] = 0
        acc2[i]["available_num"] = acc2[i][acc[i]]


    # Divide the stock into different accounts
    for i in range(len(df)):
        for j in range(len(user.columns)):
            if user.iloc[i, j] >= 100:
                for k in range(len(acc2)):
                    if acc2[k].iloc[i, 0] >= 100:
                        if acc2[k].iloc[i, 0] >= user.iloc[i, j]:
                            acc2[k].iloc[i, 0] -= user.iloc[i, j] / 100 * 100
                            acc2[k].iloc[i, j + 1] = user.iloc[i, j] / 100 * 100
                            user.iloc[i, j] = user.iloc[i, j] % 100
                        else:
                            user.iloc[i, j] -= acc2[k].iloc[i, 0] / 100 * 100
                            acc2[k].iloc[i, j + 1] = acc2[k].iloc[i, 0] / 100 * 100
                            acc2[k].iloc[i, 0] = acc2[k].iloc[i, 0] % 100

    writer = pd.ExcelWriter(divided_file)
    for i in range(len(acc)):
        acc2[i].rename(columns={acc[i]: "unallocated_num"}, inplace=True)
        acc2[i]["accountname"] = acc[i][5:]
        acc2[i]["allocated_num"] = acc2[i][
            "available_num"] - acc2[i]["unallocated_num"]
        acc2[i].reset_index(inplace=True)
        acc2[i].set_index("accountname", inplace=True)

        col = acc2[i]["allocated_num"]
        acc2[i].drop(labels=["allocated_num"], axis=1, inplace=True)
        acc2[i].insert(2, "allocated_num", col)
        col = acc2[i]["available_num"]
        acc2[i].drop(labels=["available_num"], axis=1, inplace=True)
        acc2[i].insert(2, "available_num", col)

        acc2[i].to_excel(writer, sheet_name=acc[i][5:])
    writer.close()


def run(yesterday_position_final, change_position, today_position):
    change(today_position, change_position, "changed.xlsx")
    sum(yesterday_position_final, "changed.xlsx", "summed.xlsx")
    divide("summed.xlsx", "divided.xlsx")
