#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
from io import StringIO
import os
import sys
import platform
import getpass
from sqlalchemy import create_engine, text
import datetime
from datetime import datetime, date, timedelta
import re
import numpy as np
import csv


#------------- Configuration
sysname = platform.uname()[0]
if platform.system() == 'Linux':
  print("Server")
  
  # Import Common Functions
  sys.path.append(os.path.abspath("/home/ec2-user/pa-lead/python_schedule/"))
  sys.path.append(os.path.abspath("/home/ec2-user/pa-lead/"))
  
  from functions import *
  from Query_Builder import QueryBuilder
  from Common_Functions import *
else:
    sys.path.append(os.path.abspath("/Users/abdullah.arshad/Documents/GitHub/analytics-reporting/Python Scripts/SA QueryBuilder"))
    from src.Query_Builder import QueryBuilder
    from src.Common_Functions import *

# Report/Client specific variables
user_name = 'Swifty'
user_id = 36904
#sub_advertiser_id = 104607
region = "us-east-1"
user_timezone = "US/Eastern"
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None) 
#camp = 2764737
def get_campaign_mysql_info():
    mydb = mydb_conn_func()
    mydb_q = f"""
    SELECT 
    sub_advertisers.name AS Advertiser_Name,
    campaigns.sub_advertiser_id AS Advertiser_id,
    line_items.name AS campaign_group_name,
    line_items.id AS campaign_group_id,
    campaigns.name AS campaign_name,
    campaigns.id AS campaign_id,
    campaigns.type AS campaign_type
    FROM
        campaigns
            LEFT JOIN users u ON campaigns.user_id = u.id
            LEFT JOIN line_items ON campaigns.line_item_id=line_items.id
            LEFT JOIN sub_advertisers ON campaigns.sub_advertiser_id = sub_advertisers.id
    WHERE campaigns.user_id IN ({user_id})
    GROUP by campaigns.id """
    mysql_df = pd.read_sql(mydb_q, con = mydb)
    mydb.dispose()
    return mysql_df


def get_audience_info(df):
    seg_id_list = df['segment_id'].unique().astype('str')
    seg_id_list = ','.join(f"'{x}'" for x in seg_id_list)
    mydb = mydb_conn_func()
    #CS
    cs_q = f"SELECT identifier as segment_id, CASE WHEN display_name IS NULL THEN name ELSE display_name END as audienceSegments FROM custom_segments WHERE identifier IN ({seg_id_list})"
    cs_df = pd.read_sql(cs_q,con = mydb)
    #RT
    rt_q = f"SELECT id as segment_id, name as segment_name FROM rt_segments WHERE id IN ({seg_id_list})"
    rt_df = pd.read_sql(rt_q,con = mydb)
    cs_rt_df = pd.concat([cs_df,rt_df])
    mydb.dispose()
    cs_rt_df['segment_id'] = cs_rt_df['segment_id'].astype('str')
    return cs_rt_df

def get_creative_info(df):
    nativead_id_list = df['creative_id'].unique().astype('str')
    nativead_id_list = ','.join(nativead_id_list)
    mydb = mydb_conn_func()
    creative_q = f"SELECT id as creative_id , name as creative_name FROM native_ads WHERE id IN ({nativead_id_list})"
    creative_df = pd.read_sql(creative_q,con = mydb)
    mydb.dispose()
    creative_df['creative_id'] = creative_df['creative_id'].astype('str')
    return creative_df

def deal(df):
    deal_list = df['deal_id'].unique().astype('str')
    deal_list = '","'.join(deal_list)
    mydb = mydb_conn_func()
    deal_q = f'SELECT deal_id , name as deal_name FROM programmatic_deals WHERE deal_id IN ("{deal_list}")'
    deal_df = pd.read_sql(deal_q,con = mydb)
    print(deal_df)
    deal_q = f'select deal_id from nativead_production.inventory_byod_deals where deal_id IN ("{deal_list}")'
    deal_byod = pd.read_sql(deal_q,con = mydb)
    print(deal_byod)
    mydb.dispose()
    deal_df['deal_id'] = deal_df['deal_id'].astype('str')
    deal_byod['deal_id'] = deal_byod['deal_id'].astype('str')
    return deal_df, deal_byod

def get_network_info(df):
    network_id_list = df['network_id'].unique().astype('str')
    network_id_list = ','.join(network_id_list)
    mydb = mydb_conn_func()
    network_q = f"SELECT id as network_id , company_name as network_name FROM ad_networks WHERE id IN ({network_id_list})"
    network_df = pd.read_sql(network_q,con = mydb)
    mydb.dispose()
    network_df['network_id'] = network_df['network_id'].astype('str')
    return network_df

def get_rs_data(start_date_temp, end_date_temp):
    dur = (datetime.strptime(end_date_temp,"%Y-%m-%d %H:%M:%S") - datetime.strptime(start_date_temp,"%Y-%m-%d %H:%M:%S")).days +2
    print(start_date_temp,end_date_temp, dur)
    print("Connecting to DB Engine")
    final_data = pd.DataFrame()
    # Get data from Redshift
    SELECT_stmnt = """SELECT
    request_time as date_time_utc,
    auction_id,
    ip_address_sha256,
    device_id_sha256,
    CASE WHEN lrid != '' THEN lrid else liveramp_id END as liverampid,
    network_id,
    campaign_id,
    nativead_id as creative_id,
    width as banner_width,
    height as banner_height,
    country,
    region,
    dma,
    city,
    zipcode as postal_code,
    device_geo_lat as latitude,
    device_geo_long as longitude,
    matched_cseg_and_rt as segment_id,
    demo_age_seg,
    demo_gender_seg,
    demo_houseincome_seg,
    duration,
    browser,
    device_type,
    device_make,
    device_language,
    os_family as operating_system,
    supply_inventory_type,
    conv_tracker_id,
    conv_url,
    page,
    site_publisher_name,
    site_content_genre,
    site_content_language,
    site_ref as referral_url,
    cleaned_domain_key as domain,
    sub_domain as sub_domain,
    app_categories,
    app_publisher_id,
    app_publisher_name,
    app_content_language,
    app_name,
    conv_from as conversion_from,
    s_conv_from as secondary_conversion_from,
    sum(cost)/1000000.0 as cost,
    sum(has_won) as impressions,
    sum(has_click) as clicks,
    sum(has_engagement) as engagement,
    sum(has_conv) as Primary_conversion_Conversions,
    sum(advertiser_has_conversion_deduped) as Primary_conversion_advertiser_deduped,
    sum(has_secondary_conversion) as Secondary_Conversions,
    sum(advertiser_has_secondary_conversion_deduped) as Secondary_Conversions_advertiser_deduped,
    sum(stats_revenue) as revenue,
    sum(campaign_conversion_revenue) as campaign_conversion_revenue,
    sum(video_skip) as video_skip,
    sum(stats_vcomp_0) as video_start,
    sum(stats_vcomp_25) as video_25percent,
    sum(stats_vcomp_50) as video_50percent,
    sum(stats_vcomp_75) as video_75percent,
    sum(stats_vcomp_95) as video_complete,
    sum(stats_acomp_0) as audio_start,
    sum(stats_acomp_25) as audio_25percent,
    sum(stats_acomp_50) as audio_50percent,
    sum(stats_acomp_75) as audio_75percent,
    sum(stats_acomp_95) as audio_complete,
    sum(total_time_on_site) as total_time_on_site,
    sum(CASE WHEN total_time_on_site > 0 THEN 1 ELSE 0 END) as session,
    sum(stats_moat_measure) as Measurable_View,
    CASE WHEN sum(stats_moat_measure) != 0 THEN sum(stats_moat_inview)/sum(stats_moat_measure) ELSE 0 END as Viewable_Impression
    """
    #SELECT_stmnt = censor_pii(SELECT_stmnt)
    WHERE_stmnt_non_date = f"WHERE advertiser_id IN ({user_id})"
    WHERE_stmnt = WHERE_stmnt_non_date+" AND request_time >= '"+start_date_temp+"' AND request_time < '"+end_date_temp+"'"
    GROUPBY = "GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44"
    ORDERBY = "ORDER BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44"
    end_date_temp= pd.to_datetime(end_date_temp, format="%Y-%m-%d %H:%M:%S")
    tbl_pivot1 = daily_looper_fun(dur,end_date_temp,SELECT_stmnt,WHERE_stmnt,GROUPBY,ORDERBY,user_id)
    print("Data Pull Complete - RS")
    #Fix Datatype after pull
    #tbl_pivot1.dtypes()
    tbl_pivot1['cost']=tbl_pivot1['cost'].astype(float)    
    tbl_pivot1['impressions']=tbl_pivot1['impressions'].astype(float)
    tbl_pivot1['clicks']=tbl_pivot1['clicks'].astype(float)
    tbl_pivot1['engagement']=tbl_pivot1['engagement'].astype(float)
    tbl_pivot1['primary_conversion_conversions']=tbl_pivot1['primary_conversion_conversions'].astype(float)
    tbl_pivot1['primary_conversion_advertiser_deduped']=tbl_pivot1['primary_conversion_advertiser_deduped'].astype(float)
    tbl_pivot1['secondary_conversions']=tbl_pivot1['secondary_conversions'].astype(float)
    tbl_pivot1['secondary_conversions_advertiser_deduped']=tbl_pivot1['secondary_conversions_advertiser_deduped'].astype(float)
    tbl_pivot1['revenue']=tbl_pivot1['revenue'].astype(float)
    tbl_pivot1['campaign_conversion_revenue']=tbl_pivot1['campaign_conversion_revenue'].astype(float)
    tbl_pivot1['video_skip']=tbl_pivot1['video_skip'].astype(float)
    tbl_pivot1['video_start']=tbl_pivot1['video_start'].astype(float)
    tbl_pivot1['video_25percent']=tbl_pivot1['video_25percent'].astype(float)
    tbl_pivot1['video_50percent']=tbl_pivot1['video_50percent'].astype(float)
    tbl_pivot1['video_75percent']=tbl_pivot1['video_50percent'].astype(float)
    tbl_pivot1['video_complete']=tbl_pivot1['video_complete'].astype(float)
    tbl_pivot1['audio_start']=tbl_pivot1['audio_start'].astype(float)
    tbl_pivot1['audio_25percent']=tbl_pivot1['audio_25percent'].astype(float)
    tbl_pivot1['audio_50percent']=tbl_pivot1['audio_50percent'].astype(float)
    tbl_pivot1['audio_75percent']=tbl_pivot1['audio_75percent'].astype(float)
    tbl_pivot1['audio_complete']=tbl_pivot1['audio_complete'].astype(float)
    tbl_pivot1['total_time_on_site']=tbl_pivot1['total_time_on_site'].astype(float)
    tbl_pivot1['session']=tbl_pivot1['session'].astype(float)
    tbl_pivot1['measurable_view']=tbl_pivot1['measurable_view'].astype(float)
    tbl_pivot1['viewable_impression']=tbl_pivot1['viewable_impression'].astype(float)
    #tbl_pivot1['date_time_utc'] = pd.to_datetime(tbl_pivot1['date_time_utc'],unit='s')
    print("Cast float data types to integer columns")
    #Regroup
    tbl_pivot2 = tbl_pivot1.groupby(['date_time_utc','auction_id','ip_address_sha256','device_id_sha256','liverampid','network_id','campaign_id',
                                     'creative_id','banner_width','banner_height','country','region','dma','city','postal_code','latitude','longitude','segment_id',
                                     'demo_age_seg','demo_gender_seg','demo_houseincome_seg','duration','browser','device_type','device_make',
                                     'device_language','operating_system','supply_inventory_type','conv_tracker_id','conv_url','page','site_publisher_name',
                                     'site_content_genre','site_content_language','referral_url','domain','sub_domain',
                                     'app_categories','app_publisher_id','app_publisher_name','app_content_language','app_name','conversion_from',
                                     'secondary_conversion_from',],dropna=False).sum().reset_index()
    print("Group by completed")
    #tbl_pivot2['date_time_utc'] = tbl_pivot2['date_time_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')
    print(tbl_pivot1)
    return tbl_pivot1

start_date = format((date.today()- pd.DateOffset(days=1)), "%Y-%m-%d 00:00:00")
end_date = format((date.today()- pd.DateOffset(days=1)), "%Y-%m-%d 23:59:59")
#end_date = format((date.today()- pd.DateOffset(days=0)), "%Y-%m-%d 23:59:59")

#start_date = '2025-09-04 00:00:00'
#end_date = '2025-07-30 23:59:59'
print(f'{start_date} to {end_date}')

df = get_rs_data(start_date, end_date)
df = df.drop_duplicates()

seg_map_tbl = get_audience_info(df)
df['segment_id'] = df['segment_id'].astype('str')
df = df.merge(seg_map_tbl,how ='left',on = 'segment_id')

camp_map = get_campaign_mysql_info()
df['campaign_id'] = df['campaign_id'].astype('str')
camp_map['campaign_id'] = camp_map['campaign_id'].astype('str')
df = df.merge(camp_map,how ='left',on = 'campaign_id')

creative_map = get_creative_info(df)
df['creative_id'] = df['creative_id'].astype('str')
creative_map['creative_id'] = creative_map['creative_id'].astype('str')
df = df.merge(creative_map,how ='left',on = 'creative_id')


df['auction_id_edit'] = (
    df['auctionid']
    .str.replace('_', ':', regex=False)
    .str.replace('C', '', regex=False)
    + df['conv_tracker_id']
)

df = df.drop(columns=["segment_name"])


###
csv_path = "/tmp/temp_output.csv"
df.to_csv(csv_path, index=False)
###


###################
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import gzip
import io
def send_email_with_compressed_attachment(
    sender, recipients, subject, body_text, dataframe, attachment_filename
):
    # Convert DataFrame to CSV string
    csv_data = dataframe.to_csv(index=False)
    # Compress CSV with Gzip
    buffer = io.BytesIO()
    with gzip.GzipFile(filename=attachment_filename, mode='wb', fileobj=buffer) as gz:
        gz.write(csv_data.encode('utf-8'))
    buffer.seek(0)
    compressed_data = buffer.read()
    # Create MIME email
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    # Email body
    msg_body = MIMEMultipart("alternative")
    text_part = MIMEText(body_text, "plain")
    msg_body.attach(text_part)
    msg.attach(msg_body)
    # Attach gzip file
    attachment = MIMEApplication(compressed_data)
    attachment.add_header(
        "Content-Disposition", "attachment", filename=attachment_filename + ".gz"
    )
    msg.attach(attachment)
    # Log raw message size
    raw_msg_str = msg.as_string()
    size_mb = sys.getsizeof(raw_msg_str) / 1024**2
    print(f"Raw message size: {size_mb:.2f} MB")
    # SES v2 client
    sesv2_client = boto3.client("sesv2", region_name="us-east-1")
    try:
        response = sesv2_client.send_email(
            FromEmailAddress=sender,
            Destination={"ToAddresses": recipients},
            Content={"Raw": {"Data": raw_msg_str}},
        )
        print("Email sent! Message ID:", response["MessageId"])
    except Exception as e:
        print("Error sending email:", str(e))

sender = "sa-reports@stackadapt.com"
delimiter = ","
recipients = client_list_12345
attachment_filename = "Stackadapt_LLD_reporting.csv"

#attachment_data =grouped_df_1
#subject = 'NPI Reporting from StackAdapt [BRIUMVI]'
#body_text = f'Here is your NPI Report for BRIUMVI. Data covers between {start_date} and {end_date}'

#send_email_with_compressed_attachment(    sender,    recipients,    subject,    body_text,    attachment_data,    attachment_filename,)

attachment_data =df
subject = 'LLD Report'
body_text = f'Here is your LLD Report for the date range {start_date} and {end_date}. '
send_email_with_compressed_attachment(
    sender,
    recipients,
    subject,
    body_text,
    attachment_data,
    attachment_filename,
)