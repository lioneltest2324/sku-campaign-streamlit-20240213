import streamlit as st
import pandas as pd
from streamlit_tags import st_tags
import numpy as np
from datetime import datetime, timedelta
import re
from functools import reduce
import operator
from universal_component_for_campaign import load_and_process_data
st.set_page_config(layout="wide",page_title='广告系列')
@st.cache_data(ttl=2400)
def load_data():
    sensor_url = 'https://docs.google.com/spreadsheets/d/1btySv1zKyH5zQvMXk1DqjByMU8_fQ-79OHqt67X1njI/edit#gid=0'
    ads_url = 'https://docs.google.com/spreadsheets/d/1K__Mzx-lwk7USJXMj_MdvGmO2ud_3MIJpQAZ7IAkfzU/edit#gid=0'
    ads_df = load_and_process_data(ads_url,0)
    sensor_daily_1 = load_and_process_data(sensor_url,0)
    sensor_daily_2 = load_and_process_data(sensor_url,2063213808)
    sensor_df = pd.concat([sensor_daily_1, sensor_daily_2], ignore_index=True)
    ads_df = ads_df.rename(columns={'campaignname': 'campaign'})
    ads_df['campaign'] = ads_df['campaign'].astype(str)
    # ads_df['campaignid'] = ads_df['campaignid'].astype(float)
    sensor_df = sensor_df.rename(columns={'行为时间': 'date','Campaign': 'campaign','CampaignID': 'campaignid'})
    sensor_df['campaign'] = sensor_df['campaign'].astype(str)
    sensor_df['campaignid'] = sensor_df['campaignid'].astype(float)
    return ads_df,sensor_df
@st.cache_data(ttl=2400)
def ads_data_process(df):
    df.loc[df['currency'] == 'HKD', 'cost'] *= 0.127
    df.loc[df['currency'] == 'HKD', 'ads value'] *= 0.127
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M').astype(str)
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    process_df = df.drop(columns=['currency'])
    return  process_df

@st.cache_data(ttl=2400)
def create_date_and_input_process_df(df,date_selected_range,and_tags,or_tags,exclude_tags):
    df['date'] = pd.to_datetime(df['date'])
    summary_filtered_date_range_df = df[
    (df['date'] >= pd.to_datetime(date_selected_range[0])) & (
                df['date'] <= pd.to_datetime(date_selected_range[1]))]
    or_regex = '|'.join(map(str, or_tags))  # 用于“或”筛选
    exclude_regex = '|'.join(map(str, exclude_tags))  # 用于排除
    # 普通日期内的筛选条件
    and_condition = reduce(operator.and_, [summary_filtered_date_range_df['campaign'].str.contains(tag, regex=True, flags=re.IGNORECASE) for tag in and_tags]) if and_tags else True
    or_condition = summary_filtered_date_range_df['campaign'].str.contains(or_regex, regex=True, flags=re.IGNORECASE) if or_tags else True
    exclude_condition = ~summary_filtered_date_range_df['campaign'].str.contains(exclude_regex, regex=True, flags=re.IGNORECASE) if exclude_tags else True
    combined_condition = and_condition & or_condition & exclude_condition
    condition_sku_filtered_df = summary_filtered_date_range_df[combined_condition]
    condition_sku_filtered_df['日期范围'] = pd.to_datetime(date_selected_range[0]).strftime('%Y-%m-%d')+"至"+pd.to_datetime(date_selected_range[1]).strftime('%Y-%m-%d')
    return condition_sku_filtered_df

@st.cache_data(ttl=2400)
def create_daily_filtered_date_range_df(df,date_selected_range):
    df['date'] = pd.to_datetime(df['date'])
    daily_filtered_date_range_df = df[
        (df['date'] >= pd.to_datetime(date_selected_range[0])) & (
                    df['date'] <= pd.to_datetime(date_selected_range[1]))]
    daily_filtered_date_range_df['日期范围'] = pd.to_datetime(date_selected_range[0]).strftime('%Y-%m-%d') + "至" + pd.to_datetime(date_selected_range[1]).strftime('%Y-%m-%d')
    return daily_filtered_date_range_df

@st.cache_data(ttl=2400)
def create_dynamic_column_setting(raw_select_category_df, avoid_list, percentage_list, int_list):
    column_config = {}
    for column in raw_select_category_df.columns:
        # 跳过“SKU”和“SPU”列
        if column in avoid_list:
            continue
        max_value = float(raw_select_category_df[column].max())
        if column in percentage_list:  # 百分比格式
            column_config[column] = st.column_config.ProgressColumn(
                format='%.2f%%',  # 显示为百分比
                min_value=0,
                max_value=max_value,
                label=column
            )
        elif column in int_list:  # 整数格式
            column_config[column] = st.column_config.ProgressColumn(
                format='%d',  # 显示为整数
                min_value=0,
                max_value=max_value,
                label=column
            )
        else:  # 其它列的正常处理
            column_config[column] = st.column_config.ProgressColumn(
                format='%.2f' if raw_select_category_df[column].dtype == float else '',
                # 浮点数保留两位小数
                min_value=0,
                max_value=max_value,
                label=column
            )
    return column_config

@st.cache_data(ttl=2400)
def create_ads_month_summary_df(df,flied):
    summary_df = df.groupby(flied) \
        .agg({
              'impression': 'sum',
              'cost': 'sum',
              'click': 'sum',
              'conversions': 'sum',
              'allconversions': 'sum',
              'viewconversions': 'sum',
              'ads value':'sum'
              }).reset_index()
    return summary_df

@st.cache_data(ttl=2400)
def create_sensor_month_summary_df(df,flied):
    summary_df = df.groupby(flied) \
        .agg({
        'GMV': 'sum',
        'sale': 'sum',
        'AddtoCart': 'sum',
        'saleuser': 'sum',
        'firstuser': 'sum',
        'firstuserfristbuy': 'sum',
        'uv': 'sum'
              }).reset_index()
    return summary_df

@st.cache_data(ttl=2400)
def create_ads_daily_summary_df(df,flied):
    summary_df = df.groupby(flied) \
        .agg({
              'impression': 'sum',
              'cost': 'sum',
              'click': 'sum',
              'conversions': 'sum',
              'allconversions': 'sum',
              'viewconversions': 'sum',
              'ads value':'sum'
              }).reset_index()
    return summary_df

@st.cache_data(ttl=2400)
def create_sensor_daily_summary_df(df,flied):
    summary_df = df.groupby(flied) \
        .agg({
        'GMV': 'sum',
        'sale': 'sum',
        'AddtoCart': 'sum',
        'saleuser': 'sum',
        'firstuser': 'sum',
        'firstuserfristbuy': 'sum',
        'uv': 'sum'
              }).reset_index()
    return summary_df

@st.cache_data(ttl=2400)
def merged_ads_and_sensor_monthly(ads_month_summary_df,sensor_month_summary_df):
    summary_df = pd.merge(ads_month_summary_df, sensor_month_summary_df[
        ['month', 'GMV', 'sale', 'AddtoCart', 'saleuser', 'firstuser', 'firstuserfristbuy', 'uv']], on=['month'],
             how='left')
    # 添加新的神策ROI计算列
    summary_df['神策ROI'] = (summary_df['GMV'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['神策ROI'] = summary_df['神策ROI'].fillna(0)  # 将NaN替换为0
    summary_df['神策ROI'] = summary_df['神策ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的adsROI计算列
    summary_df['ads ROI'] = (summary_df['ads value'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['ads ROI'] = summary_df['ads ROI'].fillna(0)  # 将NaN替换为0
    summary_df['ads ROI'] = summary_df['ads ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的CPC计算列
    summary_df['CPC'] = (summary_df['cost'] / summary_df['click'])
    summary_df['CPC'] = summary_df['CPC'].fillna(0)  # 将NaN替换为0
    summary_df['CPC'] = summary_df['CPC'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的客单价计算列
    summary_df['客单价'] = (summary_df['GMV'] / summary_df['sale'])
    summary_df['客单价'] = summary_df['客单价'].fillna(0)  # 将NaN替换为0
    summary_df['客单价'] = summary_df['客单价'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的点击率计算列
    summary_df['CTR'] = (summary_df['click'] / summary_df['impression']) * 100
    # 添加新的神策转化率计算列
    summary_df['神策转化率'] = (summary_df['saleuser'] / summary_df['uv']) * 100
    # 添加新的加购率计算列
    summary_df['神策加购率'] = (summary_df['AddtoCart'] / summary_df['uv']) * 100
    summary_df = summary_df.rename(columns={'cost': '费用', 'click': '点击', 'conversions': 'ads转化',
                                            'allconversions': 'ads所有转化', 'viewconversions': 'ads浏览转化',
                                            'ads value': 'ads转化价值', 'sale': '销量', 'saleuser': '支付用户数',
                                            'firstuser': '首访用户', 'firstuserfristbuy': '首访首购用户',
                                            'Sale': '销量', 'AddtoCart': '加购', 'impression': '展示', 'UV': 'UV'})
    return summary_df

@st.cache_data(ttl=2400)
def export_groupby_data(ads_month_summary_df,sensor_month_summary_df):
        summary_df = pd.merge(ads_month_summary_df, sensor_month_summary_df[
            ['date', 'campaign','GMV', 'sale', 'AddtoCart', 'saleuser', 'firstuser', 'firstuserfristbuy', 'uv']], on=['date', 'campaign'],
                              how='left')
        # 添加新的神策ROI计算列
        summary_df['神策ROI'] = (summary_df['GMV'] / summary_df['cost'])
        # 处理除以0的情况
        summary_df['神策ROI'] = summary_df['神策ROI'].fillna(0)  # 将NaN替换为0
        summary_df['神策ROI'] = summary_df['神策ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
        # 添加新的adsROI计算列
        summary_df['ads ROI'] = (summary_df['ads value'] / summary_df['cost'])
        # 处理除以0的情况
        summary_df['ads ROI'] = summary_df['ads ROI'].fillna(0)  # 将NaN替换为0
        summary_df['ads ROI'] = summary_df['ads ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
        # 添加新的客单价计算列
        summary_df['客单价'] = (summary_df['GMV'] / summary_df['sale'])
        summary_df['客单价'] = summary_df['客单价'].fillna(0)  # 将NaN替换为0
        summary_df['客单价'] = summary_df['客单价'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
        # 添加新的CPC计算列
        summary_df['CPC'] = (summary_df['cost'] / summary_df['click'])
        summary_df['CPC'] = summary_df['CPC'].fillna(0)  # 将NaN替换为0
        summary_df['CPC'] = summary_df['CPC'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
        # 添加新的点击率计算列
        summary_df['CTR'] = (summary_df['click'] / summary_df['impression']) * 100
        # 添加新的神策转化率计算列
        summary_df['神策转化率'] = (summary_df['saleuser'] / summary_df['uv']) * 100
        # 添加新的加购率计算列
        summary_df['神策加购率'] = (summary_df['AddtoCart'] / summary_df['uv']) * 100
        summary_df = summary_df.rename(columns={'cost': '费用', 'click': '点击', 'conversions': 'ads转化',
                                                'allconversions': 'ads所有转化', 'viewconversions': 'ads浏览转化',
                                                'ads value': 'ads转化价值', 'sale': '销量', 'saleuser': '支付用户数',
                                                'firstuser': '首访用户', 'firstuserfristbuy': '首访首购用户',
                                                'Sale': '销量', 'AddtoCart': '加购', 'impression': '展示', 'UV': 'UV'})
        return summary_df

@st.cache_data(ttl=2400)
def merged_ads_and_sensor_daily(ads_month_summary_df,sensor_month_summary_df):
    summary_df = pd.merge(ads_month_summary_df, sensor_month_summary_df[
        ['date', 'GMV', 'sale', 'AddtoCart', 'saleuser', 'firstuser', 'firstuserfristbuy', 'uv']], on=['date'],
             how='left')
    # 添加新的神策ROI计算列
    summary_df['神策ROI'] = (summary_df['GMV'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['神策ROI'] = summary_df['神策ROI'].fillna(0)  # 将NaN替换为0
    summary_df['神策ROI'] = summary_df['神策ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的adsROI计算列
    summary_df['ads ROI'] = (summary_df['ads value'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['ads ROI'] = summary_df['ads ROI'].fillna(0)  # 将NaN替换为0
    summary_df['ads ROI'] = summary_df['ads ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的客单价计算列
    summary_df['客单价'] = (summary_df['GMV'] / summary_df['sale'])
    summary_df['客单价'] = summary_df['客单价'].fillna(0)  # 将NaN替换为0
    summary_df['客单价'] = summary_df['客单价'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的CPC计算列
    summary_df['CPC'] = (summary_df['cost'] / summary_df['click'])
    summary_df['CPC'] = summary_df['CPC'].fillna(0)  # 将NaN替换为0
    summary_df['CPC'] = summary_df['CPC'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的点击率计算列
    summary_df['CTR'] = (summary_df['click'] / summary_df['impression']) * 100
    # 添加新的神策转化率计算列
    summary_df['神策转化率'] = (summary_df['saleuser'] / summary_df['uv']) * 100
    # 添加新的加购率计算列
    summary_df['神策加购率'] = (summary_df['AddtoCart'] / summary_df['uv']) * 100
    summary_df = summary_df.rename(columns={'cost': '费用', 'click': '点击', 'conversions': 'ads转化',
                                            'allconversions': 'ads所有转化', 'viewconversions': 'ads浏览转化',
                                            'ads value': 'ads转化价值', 'sale': '销量', 'saleuser': '支付用户数',
                                            'firstuser': '首访用户', 'firstuserfristbuy': '首访首购用户',
                                            'Sale': '销量', 'AddtoCart': '加购', 'impression': '展示', 'UV': 'UV'})
    return summary_df

@st.cache_data(ttl=2400)
def merged_ads_and_sensor_for_compare_monthly(ads_month_summary_df,sensor_month_summary_df):
    summary_df = pd.merge(ads_month_summary_df, sensor_month_summary_df[
        ['日期范围','GMV', 'sale', 'AddtoCart', 'saleuser', 'firstuser', 'firstuserfristbuy', 'uv']], on=['日期范围'],
                          how='left')
    # 添加新的神策ROI计算列
    summary_df['神策ROI'] = (summary_df['GMV'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['神策ROI'] = summary_df['神策ROI'].fillna(0)  # 将NaN替换为0
    summary_df['神策ROI'] = summary_df['神策ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的adsROI计算列
    summary_df['ads ROI'] = (summary_df['ads value'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['ads ROI'] = summary_df['ads ROI'].fillna(0)  # 将NaN替换为0
    summary_df['ads ROI'] = summary_df['ads ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的CPC计算列
    summary_df['CPC'] = (summary_df['cost'] / summary_df['click'])
    summary_df['CPC'] = summary_df['CPC'].fillna(0)  # 将NaN替换为0
    summary_df['CPC'] = summary_df['CPC'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的客单价计算列
    summary_df['客单价'] = (summary_df['GMV'] / summary_df['sale'])
    summary_df['客单价'] = summary_df['客单价'].fillna(0)  # 将NaN替换为0
    summary_df['客单价'] = summary_df['客单价'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的点击率计算列
    summary_df['CTR'] = (summary_df['click'] / summary_df['impression'])
    # 添加新的神策转化率计算列
    summary_df['神策转化率'] = (summary_df['saleuser'] / summary_df['uv'])
    # 添加新的加购率计算列
    summary_df['神策加购率'] = (summary_df['AddtoCart'] / summary_df['uv'])
    summary_df = summary_df.rename(columns={'cost': '费用', 'click': '点击', 'conversions': 'ads转化',
                                            'allconversions': 'ads所有转化', 'viewconversions': 'ads浏览转化',
                                            'ads value': 'ads转化价值', 'sale': '销量', 'saleuser': '支付用户数',
                                            'firstuser': '首访用户', 'firstuserfristbuy': '首访首购用户',
                                            'Sale': '销量', 'AddtoCart': '加购', 'impression': '展示', 'UV': 'UV'})
    # summary_df = summary_df.drop(columns=['month'])
    return summary_df

@st.cache_data(ttl=2400)
def create_compare_summary_df(origin_df,compare_df):

    # 合并 DataFrame
    combined_df = pd.concat([origin_df, compare_df], ignore_index=True)
    # 计算百分比变化
    comparison = {}
    for col in origin_df.columns:
        if col != '日期范围':
            val1 = origin_df[col].values[0]
            val2 = compare_df[col].values[0]
            # 计算百分比变化
            if val1 != 0:  # 防止除以零
                change = ((val2 - val1) / val1)
            else:
                change = ''  # 如果原值为0，则变化无穷大
            comparison[col] = change
    # 添加对比行
    comparison['日期范围'] = '对比'
    combined_df = combined_df.append(comparison, ignore_index=True)
    # 创建一个新的 DataFrame，仅复制前两行
    formatted_df = combined_df.head(2).copy()
    # 应用格式化
    formatted_df['费用'] = formatted_df['费用'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['点击'] = formatted_df['点击'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['GMV'] = formatted_df['GMV'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['ads转化价值'] = formatted_df['ads转化价值'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['神策ROI'] = formatted_df['神策ROI'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['ads ROI'] = formatted_df['ads ROI'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['销量'] = formatted_df['销量'].apply(format_first_two_rows, args=('{}',))
    formatted_df['CPC'] = formatted_df['CPC'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['CTR'] = formatted_df['CTR'].apply(format_first_two_rows, args=('{:.2%}',))
    formatted_df['神策转化率'] = formatted_df['神策转化率'].apply(format_first_two_rows, args=('{:.2%}',))
    formatted_df['神策加购率'] = formatted_df['神策加购率'].apply(format_first_two_rows, args=('{:.2%}',))
    formatted_df['首访用户'] = formatted_df['首访用户'].apply(format_first_two_rows, args=('{}',))
    formatted_df['首访首购用户'] = formatted_df['首访首购用户'].apply(format_first_two_rows, args=('{}',))
    formatted_df['uv'] = formatted_df['uv'].apply(format_first_two_rows, args=('{}',))
    formatted_df['加购'] = formatted_df['加购'].apply(format_first_two_rows, args=('{}',))
    formatted_df['展示'] = formatted_df['展示'].apply(format_first_two_rows, args=('{}',))
    formatted_df['ads转化'] = formatted_df['ads转化'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['ads所有转化'] = formatted_df['ads所有转化'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['ads浏览转化'] = formatted_df['ads浏览转化'].apply(format_first_two_rows, args=('{:.2f}',))
    formatted_df['客单价'] = formatted_df['客单价'].apply(format_first_two_rows, args=('{:.2f}',))
    # 将格式化后的前两行替换回原始 DataFrame
    compare_data_df = combined_df.iloc[2:3].copy()
    compare_data_df[compare_data_df.columns[1:]] = compare_data_df[compare_data_df.columns[1:]].apply(pd.to_numeric,
                                                                                                      errors='coerce')
    combined_df.update(formatted_df)
    combined_df.update(compare_data_df)
    return combined_df

@st.cache_data(ttl=2400)
def create_list_campaign_summary_df(daily_filtered_df,list_tags):
    condition_campaign_filtered_df = daily_filtered_df[daily_filtered_df['campaign'].isin(list_tags)]
    return condition_campaign_filtered_df

# 自定义格式化函数
@st.cache_data(ttl=2400)
def format_first_two_rows(value, format_str):
    if pd.notna(value):
        return format_str.format(value)
    return value

# 汇总列百分比处理
@st.cache_data(ttl=2400)
def format_comparison(row):
    if row['日期范围'] == '对比':
        # 只有当 '日期范围' 列的值是 '对比' 时，才进行格式化
        return [f"{x*100:.2f}%" if isinstance(x, (int, float)) and col != '日期范围' else x for col, x in row.iteritems()]
    else:
        return row  # 对于不是 '对比' 的行，返回原始行

# 汇总列样式处理
@st.cache_data(ttl=2400)
def colorize_comparison(row):
    # 新建一个与行长度相同的样式列表，初始值为空字符串
    colors = [''] * len(row)
    # 检查当前行是否为 "对比" 行
    if row['日期范围'] == '对比':
        # 遍历除了 '日期范围' 列之外的所有值
        for i, v in enumerate(row):
            # 跳过 '日期范围' 列
            if row.index[i] != '日期范围':
                try:
                    # 将字符串转换为浮点数进行比较
                    val = float(v.strip('%'))
                    if val <= 0:
                        colors[i] = 'background-color: LightCoral'
                    elif val >= 0:
                        colors[i] = 'background-color: LightGreen'
                except ValueError:
                    # 如果转换失败，说明这不是一个数值，忽略它
                    pass
    # 返回颜色样式列表
    return colors

def create_campaign_select_summary_df(ads_campaign_daily_summary_df,sensor_campaign_daily_summary_df):
    summary_df = pd.merge(ads_campaign_daily_summary_df, sensor_campaign_daily_summary_df[
        ['campaign', 'GMV', 'sale', 'AddtoCart', 'saleuser', 'firstuser', 'firstuserfristbuy', 'uv']], on=['campaign'],
                                                     how='left')
    # 添加新的神策ROI计算列
    summary_df['神策ROI'] = (summary_df['GMV'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['神策ROI'] = summary_df['神策ROI'].fillna(0)  # 将NaN替换为0
    summary_df['神策ROI'] = summary_df['神策ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的adsROI计算列
    summary_df['ads ROI'] = (summary_df['ads value'] / summary_df['cost'])
    # 处理除以0的情况
    summary_df['ads ROI'] = summary_df['ads ROI'].fillna(0)  # 将NaN替换为0
    summary_df['ads ROI'] = summary_df['ads ROI'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的CPC计算列
    summary_df['CPC'] = (summary_df['cost'] / summary_df['click'])
    summary_df['CPC'] = summary_df['CPC'].fillna(0)  # 将NaN替换为0
    summary_df['CPC'] = summary_df['CPC'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的客单价计算列
    summary_df['客单价'] = (summary_df['GMV'] / summary_df['sale'])
    summary_df['客单价'] = summary_df['客单价'].fillna(0)  # 将NaN替换为0
    summary_df['客单价'] = summary_df['客单价'].replace([np.inf, -np.inf], 0)  # 将无限值替换为0
    # 添加新的点击率计算列
    summary_df['CTR'] = (summary_df['click'] / summary_df['impression'])*100
    # 添加新的神策转化率计算列
    summary_df['神策转化率'] = (summary_df['saleuser'] / summary_df['uv'])*100
    # 添加新的加购率计算列
    summary_df['神策加购率'] = (summary_df['AddtoCart'] / summary_df['uv'])*100
    summary_df = summary_df.rename(columns={'cost': '费用', 'click': '点击', 'conversions': 'ads转化',
                                            'allconversions': 'ads所有转化', 'viewconversions': 'ads浏览转化',
                                            'ads value': 'ads转化价值', 'sale': '销量', 'saleuser': '支付用户数',
                                            'firstuser': '首访用户', 'firstuserfristbuy': '首访首购用户',
                                            'Sale': '销量', 'AddtoCart': '加购', 'impression': '展示', 'UV': 'UV'})
    summary_df = summary_df.set_index('campaign')
    return summary_df

ads_df,sensor_df = load_data()
process_ads_df = ads_data_process(ads_df)
sensor_df['date'] = pd.to_datetime(sensor_df['date'])
sensor_df['month'] = sensor_df['date'].dt.to_period('M').astype(str)
sensor_df['date'] = sensor_df['date'].dt.strftime('%Y-%m-%d')

# 日期范围
min_date = ads_df['date'].min()
min_date = datetime.strptime(min_date, "%Y-%m-%d")
ads_df['date'] = pd.to_datetime(ads_df['date'])
max_date = ads_df['date'].max()
max_date = max_date.strftime("%Y-%m-%d")
max_date = datetime.strptime(max_date, "%Y-%m-%d")
format_max_date  = max_date.strftime("%Y-%m-%d")

default_start_date = datetime.today() - timedelta(days=14)
default_end_date = datetime.today() - timedelta(days=7)
with st.sidebar:
    selected_range = st.date_input(
        "自选日期范围",
        [default_start_date, default_end_date],
        min_value=min_date,
        max_value=max_date
    )
    compare_selected_range = st.date_input(
        "对比数据日期范围",
        [default_start_date, default_end_date],
        min_value=min_date,
        max_value=max_date
    )

with st.sidebar:
    st.subheader("输入后不能用“广告系列筛选”")
    # 检查会话状态中是否有 'text' 和 'saved_text'，如果没有，初始化它们
    if 'all_campaign_text' not in st.session_state:
        st.session_state['all_campaign_text'] = ""
    if 'all_campaign_saved_text' not in st.session_state:
        st.session_state['all_campaign_saved_text'] = []

    def pass_param():
        # 保存当前文本区域的值到 'saved_text'
        if len(st.session_state['all_campaign_text']) > 0:
            separatedata = st.session_state['all_campaign_text'].split('\n')
            for singedata in separatedata:
                st.session_state['all_campaign_saved_text'].append(singedata)
        else:
            st.session_state['all_campaign_saved_text'].append(st.session_state['all_campaign_text'])
        # 清空文本区域
        st.session_state['all_campaign_text'] = ""

    def clear_area():
        st.session_state['all_campaign_saved_text'] = []

    # 创建文本区域，其内容绑定到 'text'
    input = st.text_area("批量输入系列(一个系列一行)", value=st.session_state['all_campaign_text'], key="all_campaign_text", height=10)

    # 创建一个按钮，当按下时调用 on_upper_clicked 函数
    st.button("确定", on_click=pass_param)

    list_tags = st_tags(
        label='',
        value=st.session_state['all_campaign_saved_text']  # 使用会话状态中保存的tags
    )

    st.button("清空", on_click=clear_area)

if list_tags:
    st.subheader('汇总数据')
    no_filter_expander = st.expander("广告系列历史趋势(无日期筛选)")
    default_daily_filtered_ads_df = create_daily_filtered_date_range_df(process_ads_df,['2023-01-01',format_max_date])
    default_daily_filtered_sensor_df = create_daily_filtered_date_range_df(sensor_df,['2023-01-01',format_max_date])
    default_list_campaign_condition_ads_df =  create_list_campaign_summary_df(default_daily_filtered_ads_df,list_tags)
    default_list_campaign_condition_sensor_df = create_list_campaign_summary_df(default_daily_filtered_sensor_df,list_tags)
    default_ads_month_summary_df = create_ads_month_summary_df(default_list_campaign_condition_ads_df, ['month', '日期范围'])
    default_sensor_month_summary_df = create_sensor_month_summary_df(default_list_campaign_condition_sensor_df,
                                                                     ['month', '日期范围'])
    default_ads_and_sensor_merged_monthly = merged_ads_and_sensor_monthly(default_ads_month_summary_df,
                                                                          default_sensor_month_summary_df)
    column_config = create_dynamic_column_setting(default_ads_and_sensor_merged_monthly, ['month', '日期范围'],
                                                  ['CTR', '神策转化率', '神策加购率'],
                                                  ['展示', '销量', '加购', '点击'])
    no_filter_options = no_filter_expander.multiselect(
        '选择大盘数据维度',
        default_ads_and_sensor_merged_monthly.columns,
        ['month', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
    )
    no_filter_expander.dataframe(default_ads_and_sensor_merged_monthly[no_filter_options],
                                 column_config=column_config,
                                 height=300, width=2000
                                 )
    monthly_expander = st.expander("日期筛选(月维度)")
    filtered_month_ads_df = create_daily_filtered_date_range_df(process_ads_df,selected_range)
    filtered_month_sensor_df = create_daily_filtered_date_range_df(sensor_df,selected_range)
    monthly_list_campaign_condition_ads_df =  create_list_campaign_summary_df(filtered_month_ads_df,list_tags)
    monthly_list_campaign_condition_sensor_df = create_list_campaign_summary_df(filtered_month_sensor_df,list_tags)
    ads_month_summary_df = create_ads_month_summary_df(monthly_list_campaign_condition_ads_df, ['month', '日期范围'])
    sensor_month_summary_df = create_sensor_month_summary_df(monthly_list_campaign_condition_sensor_df, ['month', '日期范围'])
    ads_and_sensor_merged_monthly = merged_ads_and_sensor_monthly(ads_month_summary_df, sensor_month_summary_df)
    daterange_filter_monthly_column_config = create_dynamic_column_setting(ads_and_sensor_merged_monthly,
                                                                           ['month', '日期范围'],
                                                                           ['CTR', '神策转化率', '神策加购率'],
                                                                           ['展示', '销量', '加购', '点击'])
    monthly_options = monthly_expander.multiselect(
        '选择月维度数据维度',
        ads_and_sensor_merged_monthly.columns,
        ['month', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
    )
    monthly_expander.dataframe(ads_and_sensor_merged_monthly[monthly_options],
                               column_config=daterange_filter_monthly_column_config,
                               height=300, width=2000
                               )
    daily_expander = st.expander("日期筛选(日维度)")
    ads_daily_summary_df = create_ads_daily_summary_df(monthly_list_campaign_condition_ads_df, ['date', '日期范围'])
    sensor_daily_summary_df = create_sensor_daily_summary_df(monthly_list_campaign_condition_sensor_df, ['date', '日期范围'])
    ads_and_sensor_merged_daily = merged_ads_and_sensor_daily(ads_daily_summary_df, sensor_daily_summary_df)
    ads_and_sensor_merged_daily['date'] = ads_and_sensor_merged_daily['date'].dt.strftime('%Y-%m-%d')
    daterange_filter_daily_column_config = create_dynamic_column_setting(ads_and_sensor_merged_daily,
                                                                         ['date', '日期范围'],
                                                                         ['CTR', '神策转化率', '神策加购率'],
                                                                         ['展示', '销量', '加购', '点击'])
    daily_options = daily_expander.multiselect(
        '选择日维度数据维度',
        ads_and_sensor_merged_daily.columns,
        ['date', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
    )
    daily_expander.dataframe(ads_and_sensor_merged_daily[daily_options],
                             column_config=daterange_filter_daily_column_config,
                             height=300, width=2000
                             )
    export_expander = st.expander("聚合数据导出")
    ads_daily_summary_df = create_ads_daily_summary_df(monthly_list_campaign_condition_ads_df, ['date', '日期范围','campaign'])
    sensor_daily_summary_df = create_sensor_daily_summary_df(monthly_list_campaign_condition_sensor_df, ['date', '日期范围','campaign'])
    ads_and_sensor_merged_daily = export_groupby_data(ads_daily_summary_df, sensor_daily_summary_df)
    ads_and_sensor_merged_daily['date'] = ads_and_sensor_merged_daily['date'].dt.strftime('%Y-%m-%d')
    ads_and_sensor_merged_daily = ads_and_sensor_merged_daily.drop(columns=['日期范围'])
    csv_data = ads_and_sensor_merged_daily.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
    export_expander.dataframe(ads_and_sensor_merged_daily)
    export_expander.download_button(
        label="下载CSV数据",
        data=csv_data,
        file_name='聚合数据.csv',
        mime='text/csv',
    )

    st.subheader("对比数据")
    compare_filtered_month_ads_df = create_daily_filtered_date_range_df(process_ads_df, compare_selected_range)
    compare_filtered_month_sensor_df = create_daily_filtered_date_range_df(sensor_df, compare_selected_range)
    compare_monthly_list_campaign_condition_ads_df = create_list_campaign_summary_df(compare_filtered_month_ads_df, list_tags)
    compare_monthly_list_campaign_condition_sensor_df = create_list_campaign_summary_df(compare_filtered_month_sensor_df, list_tags)
    ads_month_summary_df_for_compare = create_ads_month_summary_df(monthly_list_campaign_condition_ads_df, ['日期范围'])
    sensor_month_summary_df_compare = create_sensor_month_summary_df(monthly_list_campaign_condition_sensor_df, ['日期范围'])

    compare_ads_month_summary_df = create_ads_month_summary_df(compare_monthly_list_campaign_condition_ads_df, ['日期范围'])
    compare_sensor_month_summary_df = create_sensor_month_summary_df(compare_monthly_list_campaign_condition_sensor_df,
                                                             ['日期范围'])
    keep_range_summary_df = merged_ads_and_sensor_for_compare_monthly(ads_month_summary_df_for_compare,
                                                                      sensor_month_summary_df_compare)
    compare_keep_range_summary_df = merged_ads_and_sensor_for_compare_monthly(compare_ads_month_summary_df,
                                                                              compare_sensor_month_summary_df)
    compare_summary_df = create_compare_summary_df(keep_range_summary_df, compare_keep_range_summary_df)
    compare_options = st.multiselect(
        '数据维度选择',
        compare_summary_df.columns,
        ['日期范围', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
    )
    all_df = compare_summary_df[compare_options].apply(format_comparison, axis=1)
    all_df = all_df.style.apply(colorize_comparison, axis=1)
    st.dataframe(all_df)


else:
    with st.container(border=True):
        st.subheader('广告系列筛选')
        col1, col2, col3 = st.columns(3)
        # 三个条件范围
        with col1:
            and_tags = st_tags(
                label='“并”条件输入(非完全匹配)',
            )
        with col2:
            or_tags = st_tags(
                label='“或”条件输入(非完全匹配)',
            )
        with col3:
            exclude_tags = st_tags(
                label='排除条件输入(非完全匹配)',
            )
    if and_tags or or_tags or exclude_tags:
        st.subheader('汇总数据')
        no_filter_expander = st.expander("广告系列历史趋势(无日期筛选)")
        default_ads_filter_df = create_date_and_input_process_df(process_ads_df, ['2023-01-01', format_max_date], and_tags,
                                                                 or_tags, exclude_tags)
        default_sensor_filter_df = create_date_and_input_process_df(sensor_df, ['2023-01-01', format_max_date], and_tags,
                                                                    or_tags, exclude_tags)
        default_ads_month_summary_df = create_ads_month_summary_df(default_ads_filter_df, ['month', '日期范围'])
        default_sensor_month_summary_df = create_sensor_month_summary_df(default_sensor_filter_df,
                                                                         ['month', '日期范围'])
        default_ads_and_sensor_merged_monthly = merged_ads_and_sensor_monthly(default_ads_month_summary_df,
                                                                              default_sensor_month_summary_df)
        column_config = create_dynamic_column_setting(default_ads_and_sensor_merged_monthly, ['month', '日期范围'],
                                                      ['CTR', '神策转化率', '神策加购率'],
                                                      ['展示', '销量', '加购', '点击'])
        no_filter_options = no_filter_expander.multiselect(
            '选择大盘数据维度',
            default_ads_and_sensor_merged_monthly.columns,
            ['month', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        no_filter_expander.dataframe(default_ads_and_sensor_merged_monthly[no_filter_options],
                                     column_config=column_config,
                                     height=300, width=2000
                                     )
        monthly_expander = st.expander("日期筛选(月维度)")
        ads_filter_df = create_date_and_input_process_df(process_ads_df, selected_range, and_tags, or_tags,
                                                         exclude_tags)
        sensor_filter_df = create_date_and_input_process_df(sensor_df, selected_range, and_tags, or_tags, exclude_tags)
        ads_month_summary_df = create_ads_month_summary_df(ads_filter_df, ['month', '日期范围'])
        sensor_month_summary_df = create_sensor_month_summary_df(sensor_filter_df, ['month', '日期范围'])
        ads_and_sensor_merged_monthly = merged_ads_and_sensor_monthly(ads_month_summary_df, sensor_month_summary_df)
        daterange_filter_monthly_column_config = create_dynamic_column_setting(ads_and_sensor_merged_monthly,
                                                                               ['month', '日期范围'],
                                                                               ['CTR', '神策转化率', '神策加购率'],
                                                                               ['展示', '销量', '加购', '点击'])
        monthly_options = monthly_expander.multiselect(
            '选择月维度数据维度',
            ads_and_sensor_merged_monthly.columns,
            ['month', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        monthly_expander.dataframe(ads_and_sensor_merged_monthly[monthly_options],
                                   column_config=daterange_filter_monthly_column_config,
                                   height=300, width=2000
                                   )
        daily_expander = st.expander("日期筛选(日维度)")
        ads_daily_summary_df = create_ads_daily_summary_df(ads_filter_df, ['date', '日期范围'])
        sensor_daily_summary_df = create_sensor_daily_summary_df(sensor_filter_df, ['date', '日期范围'])
        ads_and_sensor_merged_daily = merged_ads_and_sensor_daily(ads_daily_summary_df, sensor_daily_summary_df)
        ads_and_sensor_merged_daily['date'] = ads_and_sensor_merged_daily['date'].dt.strftime('%Y-%m-%d')
        daterange_filter_daily_column_config = create_dynamic_column_setting(ads_and_sensor_merged_daily,
                                                                             ['date', '日期范围'],
                                                                             ['CTR', '神策转化率', '神策加购率'],
                                                                             ['展示', '销量', '加购', '点击'])
        daily_options = daily_expander.multiselect(
            '选择日维度数据维度',
            ads_and_sensor_merged_daily.columns,
            ['date', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        daily_expander.dataframe(ads_and_sensor_merged_daily[daily_options],
                                 column_config=daterange_filter_daily_column_config,
                                 height=300, width=2000
                                 )


        st.subheader('对比数据')

        compare_ads_filter_df = create_date_and_input_process_df(process_ads_df, compare_selected_range, and_tags,
                                                                 or_tags, exclude_tags)
        compare_sensor_filter_df = create_date_and_input_process_df(sensor_df, compare_selected_range, and_tags,
                                                                    or_tags, exclude_tags)

        ads_month_summary_df_for_compare = create_ads_month_summary_df(ads_filter_df, ['日期范围'])
        sensor_month_summary_df_compare = create_sensor_month_summary_df(sensor_filter_df, ['日期范围'])

        compare_ads_month_summary_df = create_ads_month_summary_df(compare_ads_filter_df, ['日期范围'])
        compare_sensor_month_summary_df = create_sensor_month_summary_df(compare_sensor_filter_df, ['日期范围'])

        keep_range_summary_df = merged_ads_and_sensor_for_compare_monthly(ads_month_summary_df_for_compare,
                                                                          sensor_month_summary_df_compare)
        compare_keep_range_summary_df = merged_ads_and_sensor_for_compare_monthly(compare_ads_month_summary_df,
                                                                                  compare_sensor_month_summary_df)
        compare_summary_df = create_compare_summary_df(keep_range_summary_df, compare_keep_range_summary_df)
        compare_options = st.multiselect(
            '数据维度选择',
            compare_summary_df.columns,
            ['日期范围', 'CPC', '点击', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        all_df = compare_summary_df[compare_options].apply(format_comparison, axis=1)
        all_df = all_df.style.apply(colorize_comparison, axis=1)
        st.dataframe(all_df)
        st.subheader('分系列数据总览')

        ads_campaign_daily_summary_df = create_ads_daily_summary_df(ads_filter_df, ['campaign'])
        sensor_campaign_daily_summary_df = create_sensor_daily_summary_df(sensor_filter_df, ['campaign'])
        campaign_select_date_range_summary_df = create_campaign_select_summary_df(ads_campaign_daily_summary_df,sensor_campaign_daily_summary_df)
        campaign_select_date_summary_column_config = create_dynamic_column_setting(campaign_select_date_range_summary_df,
                                               [''],
                                               ['CTR', '神策转化率', '神策加购率'],
                                               ['展示', '销量', '加购', '点击','uv','首访首购用户','首访用户','支付用户数'])
        campaign_select_options = st.multiselect(
            '选择广告系列数据维度',
            campaign_select_date_range_summary_df.columns,
            [ '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        st.dataframe(campaign_select_date_range_summary_df[campaign_select_options].sort_values(by='费用', ascending=False)
                     ,column_config=campaign_select_date_summary_column_config
                     )

    else:
        st.subheader('汇总数据')
        no_filter_expander = st.expander("全国家大盘数据趋势(无广告系列和日期筛选)")
        default_daily_filtered_ads_df = create_daily_filtered_date_range_df(process_ads_df,
                                                                            ['2023-01-01', format_max_date])
        default_daily_filtered_sensor_df = create_daily_filtered_date_range_df(sensor_df, ['2023-01-01', format_max_date])
        default_ads_month_summary_df = create_ads_month_summary_df(default_daily_filtered_ads_df, ['month', '日期范围'])
        default_sensor_month_summary_df = create_sensor_month_summary_df(default_daily_filtered_sensor_df,
                                                                         ['month', '日期范围'])
        default_ads_and_sensor_merged_monthly = merged_ads_and_sensor_monthly(default_ads_month_summary_df,
                                                                              default_sensor_month_summary_df)
        column_config = create_dynamic_column_setting(default_ads_and_sensor_merged_monthly, ['month', '日期范围'],
                                                      ['CTR', '神策转化率', '神策加购率'],
                                                      ['展示', '销量', '加购', '点击'])
        no_filter_options = no_filter_expander.multiselect(
            '选择大盘数据维度',
            default_ads_and_sensor_merged_monthly.columns,
            ['month', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        no_filter_expander.dataframe(default_ads_and_sensor_merged_monthly[no_filter_options],
                                     column_config=column_config,
                                     height=300, width=2000
                                     )
        monthly_expander = st.expander("日期筛选(月维度)")
        daily_filtered_ads_df = create_daily_filtered_date_range_df(process_ads_df, selected_range)
        daily_filtered_sensor_df = create_daily_filtered_date_range_df(sensor_df, selected_range)
        ads_month_summary_df = create_ads_month_summary_df(daily_filtered_ads_df, ['month', '日期范围'])
        sensor_month_summary_df = create_sensor_month_summary_df(daily_filtered_sensor_df, ['month', '日期范围'])
        ads_and_sensor_merged_monthly = merged_ads_and_sensor_monthly(ads_month_summary_df, sensor_month_summary_df)
        daterange_filter_monthly_column_config = create_dynamic_column_setting(ads_and_sensor_merged_monthly,
                                                                               ['month', '日期范围'],
                                                                               ['CTR', '神策转化率', '神策加购率'],
                                                                               ['展示', '销量', '加购', '点击'])
        monthly_options = monthly_expander.multiselect(
            '选择月维度数据维度',
            ads_and_sensor_merged_monthly.columns,
            ['month', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        monthly_expander.dataframe(ads_and_sensor_merged_monthly[monthly_options],
                                   column_config=daterange_filter_monthly_column_config,
                                   height=300, width=2000
                                   )
        daily_expander = st.expander("日期筛选(日维度)")
        ads_daily_summary_df = create_ads_daily_summary_df(daily_filtered_ads_df, ['date', '日期范围'])
        sensor_daily_summary_df = create_sensor_daily_summary_df(daily_filtered_sensor_df, ['date', '日期范围'])
        ads_and_sensor_merged_daily = merged_ads_and_sensor_daily(ads_daily_summary_df, sensor_daily_summary_df)
        ads_and_sensor_merged_daily['date'] = ads_and_sensor_merged_daily['date'].dt.strftime('%Y-%m-%d')
        daterange_filter_daily_column_config = create_dynamic_column_setting(ads_and_sensor_merged_daily,
                                                                             ['date', '日期范围'],
                                                                             ['CTR', '神策转化率', '神策加购率'],
                                                                             ['展示', '销量', '加购', '点击'])
        daily_options = daily_expander.multiselect(
            '选择日维度数据维度',
            ads_and_sensor_merged_daily.columns,
            ['date', '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        daily_expander.dataframe(ads_and_sensor_merged_daily[daily_options],
                                 column_config=daterange_filter_daily_column_config,
                                 height=300, width=2000
                                 )
        st.subheader('分系列数据总览')
        ads_campaign_daily_summary_df = create_ads_daily_summary_df(daily_filtered_ads_df, ['campaign'])
        sensor_campaign_daily_summary_df = create_sensor_daily_summary_df(daily_filtered_sensor_df, ['campaign'])
        campaign_select_date_range_summary_df = create_campaign_select_summary_df(ads_campaign_daily_summary_df,sensor_campaign_daily_summary_df)
        campaign_select_date_summary_column_config = create_dynamic_column_setting(campaign_select_date_range_summary_df,
                                               [''],
                                               ['CTR', '神策转化率', '神策加购率'],
                                               ['展示', '销量', '加购', '点击','uv','首访首购用户','首访用户','支付用户数'])
        campaign_select_options = st.multiselect(
            '选择广告系列数据维度',
            campaign_select_date_range_summary_df.columns,
            [ '费用', 'CPC', 'GMV', '神策ROI', '客单价', '神策转化率']
        )
        st.dataframe(campaign_select_date_range_summary_df[campaign_select_options].sort_values(by='费用', ascending=False)
                     ,column_config=campaign_select_date_summary_column_config
                     )
