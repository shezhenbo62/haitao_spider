# coding:utf-8

# 对现有D88品牌库进行品牌名处理，使品牌名适用于天猫结果搜索
import pandas as pd

pd.set_option('mode.chained_assignment', None)


def d88_brand_handle():
    pd_tm = pd.read_excel("C:/Users/Administrator/Desktop/品牌库分类.xlsx",
                          sheet_name=[0, 1, 2, 4, 5], usecols=[1, 2], names=['brand_name', 'brand_type'])
    category_name = ['美妆', '服饰', '高端服饰', '母婴', '零食']
    i = 0
    info_list = []
    for info in pd_tm.values():
        info['category'] = category_name[i]
        i += 1
        info_list.append(info)
    df = pd.concat(info_list, ignore_index=True)
    print(len(df))
    order = ['category', 'brand_name', 'brand_type']
    df = df[order]
    df = df.dropna(how='all', subset=['brand_type'])
    cn_brand_list = []
    en_brand_list = []
    for brand in df.values.tolist():
        # brand = str(brand)
        split_list = []
        if ('\u4e00' <= str(brand[1]) <= '\u9fff' and str(brand[1]).isalpha()):
            cn_brand_list.append([brand[0], brand[1].lower(), brand[2]])
        else:
            for j in str(brand[1]):
                if not ('\u4e00' <= j <= '\u9fff' or j == '/'):
                    split_list.append(j)
            merge_str = ''.join(split_list)
            merge_str = merge_str.strip()
            en_brand_list.append([brand[0], merge_str.lower(), brand[2]])
    all_list = en_brand_list + cn_brand_list
    print(all_list)
    result = pd.DataFrame(all_list, columns=['category', 'brand_name', 'brand_type'])
    result.to_excel("C:/Users/Administrator/Desktop/D88处理过后品牌名1.xls", index=False)


def database_brand_handle():
    pd_tm = pd.read_csv("C:/Users/Administrator/Desktop/d88_brand.csv", usecols=[0, 1], names=['brand_id', 'brand_name'])
    cn_brand_list = []
    en_brand_list = []
    for brand in pd_tm.values.tolist():
        # brand = str(brand)
        split_list = []
        if all(['\u4e00' <= i <= '\u9fff' for i in str(brand[1])]):
            cn_brand_list.append([brand[0], brand[1].lower()])
        elif all(['a' <= j <= 'z' for j in str(brand[1])]):
            en_brand_list.append([brand[0], brand[1].lower()])
        else:
            for j in str(brand[1]):
                if not ('\u4e00' <= j <= '\u9fff' or j == '/' or j == '·'):
                    split_list.append(j)
            merge_str = ''.join(split_list)
            merge_str = merge_str.strip()
            en_brand_list.append([brand[0], merge_str.lower()])
    all_list = en_brand_list + cn_brand_list
    result = pd.DataFrame(all_list, columns=['brand_id', 'brand_name_change'])
    merge_result = pd.merge(result, pd_tm, how='inner', on=['brand_id'])
    merge_result = merge_result[['brand_id', 'brand_name', 'brand_name_change']]
    print(merge_result)
    # merge_result.to_excel("C:/Users/Administrator/Desktop/D88111.xls", index=False)
    merge_result.to_csv("C:/Users/Administrator/Desktop/D885461.csv", index=False)


if __name__ == '__main__':
    # d88_brand_handle()
    database_brand_handle()
    # matching_brand()