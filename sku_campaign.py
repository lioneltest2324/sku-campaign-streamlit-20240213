import streamlit as st
import pandas as pd
from universal_component_for_campaign import load_and_process_data,process_usfeed_and_hmfeed_sku_on_ads_data,process_hk_cost_and_value_on_ads_data,\
    process_old_new_sku_2022_and_2023_on_ads_data,merged_spu_to_sku_on_ads_data,merged_imagelink_to_sku_on_ads_data,create_date_filtered_df,\
    output_groupby_df,out_date_range_data,add_groupby_sum_columns_to_list_df,create_dynamic_column_setting,add_custom_proportion_to_df,add_custom_proportion_to_df_x100,\
    create_sensor_gmv_filter_input,create_bulk_sku_input,create_sensor_campaign_filter_input_df,condition_evaluate,merged_saleprice_to_sku_on_ads_data,\
    create_compare_summary_df,format_first_two_rows,format_comparison,colorize_comparison
st.set_page_config(layout="wide")
# ---------------------------------------------------------------------基础数据处理区开始---------------------------------------------------------------------------------------------------
sensor_url = 'https://docs.google.com/spreadsheets/d/1X0YPC6iAZn1Lu4szX67fi5h4B8HiVbfA-i68EyzpOq0/edit#gid=0'
ads_url = 'https://docs.google.com/spreadsheets/d/13G1sZWVLKa_kpScqGVmNp-5abCTkxmAFW0dxW29DMUY/edit#gid=0'
spu_index_url = "https://docs.google.com/spreadsheets/d/1bQTrtNC-o9etJ3xUwMeyD8m383xRRq9U7a3Y-gxjP-U/edit#gid=455883801"

ads_daily_df = load_and_process_data(ads_url,0)
sensor_daily = load_and_process_data(sensor_url,0)
spu_index = load_and_process_data(spu_index_url,455883801)
old_new = load_and_process_data(spu_index_url,666585210)
ads_daily_df= process_usfeed_and_hmfeed_sku_on_ads_data(ads_daily_df,'MC ID',569301767,9174985,'SKU')
ads_daily_df= process_hk_cost_and_value_on_ads_data(ads_daily_df,'Currency','cost','ads value','HKD')
ads_daily_df = process_old_new_sku_2022_and_2023_on_ads_data(ads_daily_df,'customlabel1')
ads_daily_df['SKU'] = ads_daily_df['SKU'].str.strip().str.replace('\n', '').replace('\t', '').str.upper()
ads_daily_df = merged_spu_to_sku_on_ads_data(ads_daily_df,spu_index,'SKU', 'SPU')
old_new  = old_new.rename(columns={'SKU ID':'SKU'})
sensor_daily  = sensor_daily.rename(columns={'行为时间':'Date'})
ads_daily_df  = ads_daily_df.rename(columns={'Campaign Name':'Campaign'})
ads_daily_df = merged_imagelink_to_sku_on_ads_data(ads_daily_df,old_new,'SKU', 'imagelink')
ads_daily_df = merged_saleprice_to_sku_on_ads_data(ads_daily_df,old_new,'SKU', 'Sale Price')
ads_daily_df = output_groupby_df(ads_daily_df,['Campaign','SKU', 'SPU', 'Date','imagelink','Sale Price'], ['impression','cost','click','conversions','ads value'], 'sum').reset_index()
sensor_daily = merged_spu_to_sku_on_ads_data(sensor_daily,spu_index,'SKU', 'SPU')
unique_campaign = ads_daily_df['Campaign'].unique()
campaign_options = st.multiselect(
    '选择广告系列',
    unique_campaign
)
# 日期选择框
selected_range = out_date_range_data(ads_daily_df,'Date',"自选日期范围")
# 选择日期范围内的数据
sensor_daily['Date'] = pd.to_datetime(sensor_daily['Date'])
ads_daily_df['Date'] = pd.to_datetime(ads_daily_df['Date'])
seonsor_daily_filtered_date_range_df = create_date_filtered_df(sensor_daily,'Date',selected_range)
ads_daily_filtered_date_range_df = create_date_filtered_df(ads_daily_df,'Date',selected_range)
ads_daily_filtered_date_range_df = output_groupby_df(ads_daily_filtered_date_range_df,
['Campaign','SKU', 'SPU', 'Date','imagelink','Sale Price'],
['impression', 'cost', 'click', 'conversions', 'ads value'], 'sum').reset_index()
seonsor_daily_filtered_date_range_df['Date'] = seonsor_daily_filtered_date_range_df['Date'].dt.strftime('%Y-%m-%d')
ads_daily_filtered_date_range_df['Date'] = ads_daily_filtered_date_range_df['Date'].dt.strftime('%Y-%m-%d')
seonsor_daily_filter_campaign_select_df = seonsor_daily_filtered_date_range_df[seonsor_daily_filtered_date_range_df['Campaign'].isin(campaign_options)]
ads_daily_filter_campaign_select_df = ads_daily_filtered_date_range_df[ads_daily_filtered_date_range_df['Campaign'].isin(campaign_options)]
summary_df = pd.merge(ads_daily_filter_campaign_select_df,
                      seonsor_daily_filter_campaign_select_df[['Date', 'Campaign','SKU', 'GMV', 'UV', 'AddtoCart', 'saleuser', 'sale']],
                      on=['Date', 'Campaign','SKU'], how='left')
daily_expander = st.expander("源数据导出")
daily_expander.dataframe(summary_df)
daily_csv_data = summary_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
daily_expander.download_button(
    label="下载日维度CSV数据",
    data=daily_csv_data,
    file_name='日维度CSV数据.csv',
    mime='text/csv',
)
# ---------------------------------------------------------------------基础数据处理区结束---------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------汇总数据表开始---------------------------------------------------------------------------------------------------
summary_all_df = output_groupby_df(summary_df, ['SKU', 'SPU','imagelink','Sale Price'],
['impression','cost','click','conversions','ads value','saleuser','sale','GMV','UV','AddtoCart'], 'sum').reset_index()
summary_all_df = add_custom_proportion_to_df(summary_all_df,'GMV','cost','神策ROI')
summary_all_df= add_custom_proportion_to_df(summary_all_df,'ads value','cost','ads ROI')
summary_all_df = add_custom_proportion_to_df(summary_all_df,'cost','click','CPC')
summary_all_df = add_custom_proportion_to_df(summary_all_df, 'cost', 'conversions', 'ads CPA')
summary_all_df = add_custom_proportion_to_df_x100(summary_all_df,'click','impression','CTR')
summary_all_df= add_custom_proportion_to_df_x100(summary_all_df,'sale','UV','神策转化率')
summary_all_df = add_custom_proportion_to_df_x100(summary_all_df,'AddtoCart','UV','神策加购率')
summary_all_df_column_config = create_dynamic_column_setting(summary_all_df,['SKU', 'SPU','Sale Price'],
['imagelink'],['conversions', 'ads value', 'GMV', 'AddtoCart', 'saleuser','神策ROI', 'ads ROI', 'CPC', 'ads CPA', 'cost'],
['CTR', '神策转化率', '神策加购率'],['impression', 'click', 'sale', 'UV'], None, None)
summary_options = st.multiselect(
    '选择汇总数据维度',
    summary_all_df.columns,
    ['SKU','imagelink','Sale Price','cost','ads value','GMV','ads ROI','神策ROI']
)


st.dataframe(summary_all_df[summary_options]
             ,column_config=summary_all_df_column_config,width=1600, height=500)
summary_csv_data = summary_all_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
st.download_button(
    label="下载汇总CSV数据",
    data=summary_csv_data,
    file_name='汇总CSV数据.csv',
    mime='text/csv',
)
# ---------------------------------------------------------------------汇总数据表结束---------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------趋势数据表开始---------------------------------------------------------------------------------------------------
before_list_summary_df = output_groupby_df(summary_df,['SKU', 'SPU', 'Date','imagelink','Sale Price'], ['impression','cost','click','conversions','ads value', 'GMV','UV', 'AddtoCart', 'saleuser', 'sale'], 'sum').reset_index()
before_list_summary_df = add_custom_proportion_to_df(before_list_summary_df,'GMV','cost','神策ROI')
before_list_summary_df= add_custom_proportion_to_df(before_list_summary_df,'ads value','cost','ads ROI')
before_list_summary_df = add_custom_proportion_to_df(before_list_summary_df,'cost','click','CPC')
before_list_summary_df = add_custom_proportion_to_df(before_list_summary_df, 'cost', 'conversions', 'ads CPA')
before_list_summary_df = add_custom_proportion_to_df_x100(before_list_summary_df,'click','impression','CTR')
before_list_summary_df= add_custom_proportion_to_df_x100(before_list_summary_df,'sale','UV','神策转化率')
before_list_summary_df = add_custom_proportion_to_df_x100(before_list_summary_df,'AddtoCart','UV','神策加购率')
summary_list_df = output_groupby_df(before_list_summary_df, ['SKU', 'SPU','imagelink','Sale Price'],
['Date','impression','cost','click','conversions','ads value','saleuser','sale','GMV','UV','AddtoCart','神策ROI','ads ROI','CPC','CTR','神策转化率','神策加购率'], list).reset_index()
summary_list_df = add_groupby_sum_columns_to_list_df(summary_df,summary_list_df ,['SKU'],'cost','cost_sum')
summary_list_df = add_groupby_sum_columns_to_list_df(summary_df,summary_list_df ,['SKU'],'GMV','gmv_sum')
summary_list_df = add_groupby_sum_columns_to_list_df(summary_df,summary_list_df ,['SKU'],'ads value','ads_value_sum')
summary_list_df = add_custom_proportion_to_df(summary_list_df,'gmv_sum','cost_sum','神策总ROI').round(2)
summary_list_df = add_custom_proportion_to_df(summary_list_df,'ads_value_sum','cost_sum','ads总ROI').round(2)
summary_list_df_column_config = create_dynamic_column_setting(summary_list_df,['SKU', 'SPU','Sale Price','Date'],
['imagelink'], ['cost_sum', 'gmv_sum','ads_value_sum','神策总ROI','ads总ROI'], [''], [''], None, None)
summary_list_options = st.multiselect(
    '选择趋势数据维度',
    summary_list_df.columns,
    ['SKU','imagelink','Sale Price','cost_sum', 'gmv_sum','ads_value_sum','神策总ROI','ads总ROI','impression','cost','click','conversions','ads value']
)
st.dataframe(summary_list_df[summary_list_options]
             ,column_config=summary_list_df_column_config,width=1600, height=500)
# ---------------------------------------------------------------------趋势数据表结束---------------------------------------------------------------------------------------------------
