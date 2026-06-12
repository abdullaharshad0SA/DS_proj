Hey Sam, Just to mention for most of these python scripts, I do use a function called `daily_looper_fun()`. I don't go over this extenively in the script but this script determines which Redshift shard the data lives in (based on the advertiser id), it also censors certain fields based on the geographic region of the user and whether they had cookies enables on the site when they visit the site. This is a security/PII check we enforce on any data pull. This fuction is maintained by me and as legal implements new rules, I do make changes. 

Here is what every SQL query needs to go thru before its pulls from redshift:

def censor_pii(input_select_stmnt, timezone="UTC", hour_trunc=False, need_latlong=False):
    # Cleaning input_select_stmnt - Remove spaces between text and a comma
    input_select_stmnt = re.sub(" +(?=,)", "", input_select_stmnt)

    # Columns to exclude from the SELECT statement
    never_share_cols = [
        "device_id_sha",
        "device_id_md5",
        "device_ifa",
        "win_price",
        "bid_price",
        "bid_reduction",
        "margin",
        "margin_adjusted",
        "conv_order_id",
        "email_recipient_address",
    ]
    never_share_pattern = "|".join(never_share_cols)
    input_select_stmnt = re.sub(never_share_pattern, "", input_select_stmnt)
    globals()["SELECT_groups"] = input_select_stmnt

    us_state_excl_cols = ["device_geo_lat", "device_geo_long"]
    us_safe_pii_cols = [
        "ip_address",
        "device_id",
        "last_ip",
        "user_agent",
        "liveramp_id",
        "request_duid",
        "most_associated_duid",
        "most_associated_ip",
        "most_associated_duid_tier",
        "request_duid_hashed",
        "most_associated_duid_hashed",
        "most_associated_ip_hashed",
        "hashed_ip",
    ]

    pii_cols = us_state_excl_cols + us_safe_pii_cols

    # other_cols is for alias removal use only
    other_cols = ["zipcode", "network_id"]

    # Combine all the columns to match pii columns only to remove aliases
    columns = pii_cols + other_cols

    for _ in columns:
        input_select_stmnt = re.sub(
            r"\b{}\b\s+AS\s+\w+".format(re.escape(_)),
            _,
            input_select_stmnt,
            flags=re.IGNORECASE,
        )

    pii_sharing_countries = "'in','us','pk','ng','bd','de','gb','fr','it','ke','es','sa','pe','my','au','cl','nl','cz','pt','se','ae','ch','lb','sg','no','nz','kw','jm','bn','ca'"
    us_state_loc_drop = "'ct', 'va'"
    output_select_stmnt = input_select_stmnt

    # if zipcode in input_select_stmnt then add case when statement that removes anything over 5 digits if country is US
    zip_col_update = """CASE
        WHEN country = 'us' THEN LEFT(zipcode, 5)
        WHEN country = 'ca' THEN LEFT(zipcode, 3)
        ELSE zipcode
          END AS zipcode"""

    output_select_stmnt = output_select_stmnt.replace("zipcode", zip_col_update)
    # request_time when there is a conversion

    if hour_trunc:
        conv_timestamp_stmnt = (
            f" DATE_TRUNC('hour', convert_timezone('{timezone}', request_time)) "
        )

        output_select_stmnt = output_select_stmnt.replace(
            "request_time", conv_timestamp_stmnt
        )
    else:
        print(
            "Warning: If you have included request_time wihtout date_trunc + convert timezone please set hour_trunc = True !!!"
        )

    if need_latlong:
        print(
            "Warning: Pulling LatLong! Please make sure that you are not sharing any PII col with Lat Long!!!"
        )
    else:
        for pii_col in pii_cols:
            if re.search(pii_col, output_select_stmnt):
                print("Warning: Found device lat/long with PII columns, removing it!!!")
                # Remove device_geo_lat and device_geo_long from the statement
                output_select_stmnt = re.sub(
                    r"\s*device_geo_lat\s*,?", "", output_select_stmnt
                )
                output_select_stmnt = re.sub(
                    r"\s*device_geo_long\s*,?", "", output_select_stmnt
                )

    for pii_col in pii_cols:
        if pii_col in us_state_excl_cols:
            us_state_loc_drop_update = f"""
                CASE
                    WHEN (country = 'us' AND region IN ({us_state_loc_drop},'')) OR network_id = 211 OR device_dnt = 1 THEN NULL
                    WHEN country NOT IN ({pii_sharing_countries}) OR country = '' OR network_id = 211 THEN NULL
                    ELSE {pii_col}
                END AS {pii_col}
            """
            output_select_stmnt = output_select_stmnt.replace(
                pii_col, us_state_loc_drop_update
            )
        else:
            if pii_col == "ip_address":
                us_safe_col_update = f"""
                CASE
                    WHEN has_conv = 1
                        OR has_secondary_conversion = 1
                        OR line_item_has_secondary_conversion_deduped = 1
                        OR advertiser_has_secondary_conversion_deduped = 1
                        OR has_ltconv = 1
                        OR has_s_ltconv = 1
                    THEN ip_address_sha256
                    WHEN country NOT IN ({pii_sharing_countries}) OR country ='' OR network_id = 211 OR device_dnt = 1 THEN NULL
                    ELSE {pii_col} 
                  END AS {pii_col} 
                  """
                output_select_stmnt = output_select_stmnt.replace(
                    f"{pii_col},", f"{us_safe_col_update},"
                )
            else:
                us_safe_col_update = f"""
                    CASE
                        WHEN country NOT IN ({pii_sharing_countries}) OR country = '' OR network_id = 211 OR device_dnt = 1 THEN NULL
                        ELSE {pii_col}
                    END AS {pii_col}
                """
                output_select_stmnt = output_select_stmnt.replace(
                    f"{pii_col},", f"{us_safe_col_update},"
                )

    return output_select_stmnt

