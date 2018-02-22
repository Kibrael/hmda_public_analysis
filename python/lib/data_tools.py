from collections import OrderedDict
import pandas as pd
import psycopg2


def connect():
    """Creates a connection to a local PG database."""
    #parameter format for local use
    with open("../../pg_pw.txt", 'r') as pw_file:
        pw = pw_file.readline()

    params = {
    'dbname':'postgres',
    'user':'postgres',
    'password':pw,
    'host':'localhost'}

    try:
        conn = psycopg2.connect(**params)
        #print("Connected!")
        return conn.cursor("turtle") #returns connection and cursor
    except psycopg2.Error as e: #if database connection results in an error print the following
        print("I am unable to connect to the database: ", e)

def get_sql_results_df(table, sql, cur, table2=None):
    """formats SQL script and pulls data from the passed table(s)."""
    if not table2:
        sql=sql.format(table=table)
    else:
        table2 = table[:4] + str(int(table[4:8])-1) + "_ffiec"
        sql=sql.format(table=table, table2=table2)
    cur.execute(sql)
    colnames = [desc[0] for desc in cur.description]
    data_df = pd.DataFrame(cur.fetchall(), columns=colnames)
    return data_df

def compile_dfs(tables, outfile, sql, table2=None):
    """Uses get_sql_results_df to compile multiple years of data (frames) into a single dataframe"""
    cur=connect()
    first = True
    for table in tables:
        year = table[4:8]
        data_df = get_sql_results_df(table, sql, cur, table2)
        data_df["year"] = year
        print(data_df.head())
        if first:
            first = False
            data_out_df = data_df.copy()
        else:
            data_out_df = pd.concat([data_out_df, data_df])
    data_out_df.to_csv("../output/"+outfile+".csv", sep="|", index=False)
    cur.close()
    return data_out_df

def get_lar_stats(field, table_list, where=None):
    """Loops over table_list and aggregates the metrics specified in get_year_stats."""
    historic_metrics = []
    for table in table_list:
        print("pulling metrics for:", table)
        year_data = get_year_stats(field=field, table=table, where=where)
        historic_metrics.append(year_data)
    return historic_metrics

def get_year_stats(field, table, where=None):
    """Aggregates numeric statistics for the specified field for one LAR year, specified by table"""
    year_data = OrderedDict({})
    raw_data = get_field_data(field, table, where)
    year_data["sum_all"] = get_field_sum(field, table, where)
    year_data["count_all"] = get_field_count(field, table, where)
    year_data["mean"] = get_field_mean(field, table, where)
    year_data["stdev"] = get_field_stdev(field, table, where)
    year_data["median"] = raw_data[field].median()
    year_data["skew"] = raw_data[field].skew()
    year_data["kurtosis"] = raw_data[field].kurtosis()
    year_data["max"] = raw_data[field].map(lambda x: int(x)).max()
    year_data["min"] = raw_data[field].map(lambda x: int(x)).min()
    year_data["pct_10"] = get_percentile(field, table, pct=.1, where=where)
    year_data["pct_20"] = get_percentile(field, table, pct=.2, where=where)
    year_data["pct_30"] = get_percentile(field, table, pct=.3, where=where)
    year_data["pct_40"] = get_percentile(field, table, pct=.4, where=where)
    year_data["pct_50"] = get_percentile(field, table, pct=.5, where=where)
    year_data["pct_60"] = get_percentile(field, table, pct=.6, where=where)
    year_data["pct_70"] = get_percentile(field, table, pct=.7, where=where)
    year_data["pct_80"] = get_percentile(field, table, pct=.8, where=where)
    year_data["pct_90"] = get_percentile(field, table, pct=.9, where=where)
    #add race filters to subsets
    race_1_where = where + """ AND (race_1 = '1' OR race_2 = '1' OR race_3 = '1' OR race_4 = '1' OR race_5 = '1' OR
    co_race_1 = '1' OR co_race_2 = '1' OR co_race_3 = '1' OR co_race_4 = '1' OR co_race_5 = '1')"""
    raw_data = get_field_data(field, table,race_1_where)
    year_data["sum_native"] = get_field_sum(field, table, race_1_where)
    year_data["count_native"] = get_field_count(field, table, race_1_where)
    year_data["mean_native"] = get_field_mean(field, table, race_1_where)
    year_data["stdev_native"] = get_field_stdev(field, table, race_1_where)
    year_data["median_native"] = raw_data[field].median()
    year_data["skew_native"] = raw_data[field].skew()
    year_data["kurtosis_native"] = raw_data[field].kurtosis()
    year_data["max_native"] = raw_data[field].map(lambda x: int(x)).max()
    year_data["min_native"] = raw_data[field].map(lambda x: int(x)).min()
    year_data["pct_10_native"] = get_percentile(field, table, pct=.1, where=race_1_where)
    year_data["pct_20_native"] = get_percentile(field, table, pct=.2, where=race_1_where)
    year_data["pct_30_native"] = get_percentile(field, table, pct=.3, where=race_1_where)
    year_data["pct_40_native"] = get_percentile(field, table, pct=.4, where=race_1_where)
    year_data["pct_50_native"] = get_percentile(field, table, pct=.5, where=race_1_where)
    year_data["pct_60_native"] = get_percentile(field, table, pct=.6, where=race_1_where)
    year_data["pct_70_native"] = get_percentile(field, table, pct=.7, where=race_1_where)
    year_data["pct_80_native"] = get_percentile(field, table, pct=.8, where=race_1_where)
    year_data["pct_90_native"] = get_percentile(field, table, pct=.9, where=race_1_where)

    race_2_where = where + """ AND (race_1 = '2' OR race_2 = '2' OR race_3 = '2' OR race_4 = '2' OR race_5 = '2' OR
    co_race_1 = '2' OR co_race_2 = '2' OR co_race_3 = '2' OR co_race_4 = '2' OR co_race_5 = '2')"""
    raw_data = get_field_data(field, table,race_2_where)
    year_data["sum_asian"] = get_field_sum(field, table, race_2_where)
    year_data["count_asian"] = get_field_count(field, table, race_2_where)
    year_data["mean_asian"] = get_field_mean(field, table, race_2_where)
    year_data["stdev_asian"] = get_field_stdev(field, table, race_2_where)
    year_data["median_asian"] = raw_data[field].median()
    year_data["skew_asian"] = raw_data[field].skew()
    year_data["kurtosis_asian"] = raw_data[field].kurtosis()
    year_data["max_asian"] = raw_data[field].map(lambda x: int(x)).max()
    year_data["min_asian"] = raw_data[field].map(lambda x: int(x)).min()
    year_data["pct_10_asian"] = get_percentile(field, table, pct=.1, where=race_2_where)
    year_data["pct_20_asian"] = get_percentile(field, table, pct=.2, where=race_2_where)
    year_data["pct_30_asian"] = get_percentile(field, table, pct=.3, where=race_2_where)
    year_data["pct_40_asian"] = get_percentile(field, table, pct=.4, where=race_2_where)
    year_data["pct_50_asian"] = get_percentile(field, table, pct=.5, where=race_2_where)
    year_data["pct_60_asian"] = get_percentile(field, table, pct=.6, where=race_2_where)
    year_data["pct_70_asian"] = get_percentile(field, table, pct=.7, where=race_2_where)
    year_data["pct_80_asian"] = get_percentile(field, table, pct=.8, where=race_2_where)
    year_data["pct_90_asian"] = get_percentile(field, table, pct=.9, where=race_2_where)

    race_3_where = where + """ AND (race_1 = '3' OR race_2 = '3' OR race_3 = '3' OR race_4 = '3' OR race_5 = '3' OR
    co_race_1 = '3' OR co_race_2 = '3' OR co_race_3 = '3' OR co_race_4 = '3' OR co_race_5 = '3')"""
    raw_data = get_field_data(field, table,race_3_where)
    year_data["sum_black"] = get_field_sum(field, table, race_3_where)
    year_data["count_black"] = get_field_count(field, table, race_3_where)
    year_data["mean_black"] = get_field_mean(field, table, race_3_where)
    year_data["stdev_black"] = get_field_stdev(field, table, race_3_where)
    year_data["median_black"] = raw_data[field].median()
    year_data["skew_black"] = raw_data[field].skew()
    year_data["kurtosis_black"] = raw_data[field].kurtosis()
    year_data["max_black"] = raw_data[field].map(lambda x: int(x)).max()
    year_data["min_black"] = raw_data[field].map(lambda x: int(x)).min()
    year_data["pct_10_black"] = get_percentile(field, table, pct=.1, where=race_3_where)
    year_data["pct_20_black"] = get_percentile(field, table, pct=.2, where=race_3_where)
    year_data["pct_30_black"] = get_percentile(field, table, pct=.3, where=race_3_where)
    year_data["pct_40_black"] = get_percentile(field, table, pct=.4, where=race_3_where)
    year_data["pct_50_black"] = get_percentile(field, table, pct=.5, where=race_3_where)
    year_data["pct_60_black"] = get_percentile(field, table, pct=.6, where=race_3_where)
    year_data["pct_70_black"] = get_percentile(field, table, pct=.7, where=race_3_where)
    year_data["pct_80_black"] = get_percentile(field, table, pct=.8, where=race_3_where)
    year_data["pct_90_black"] = get_percentile(field, table, pct=.9, where=race_3_where)

    race_4_where = where + """ AND (race_1 = '4' OR race_2 = '4' OR race_3 = '4' OR race_4 = '4' OR race_5 = '4' OR
    co_race_1 = '4' OR co_race_2 = '4' OR co_race_3 = '4' OR co_race_4 = '4' OR co_race_5 = '4')"""
    raw_data = get_field_data(field, table,race_4_where)
    year_data["sum_islander"] = get_field_sum(field, table, race_4_where)
    year_data["count_islander"] = get_field_count(field, table, race_4_where)
    year_data["mean_islander"] = get_field_mean(field, table, race_4_where)
    year_data["stdev_islander"] = get_field_stdev(field, table, race_4_where)
    year_data["median_islander"] = raw_data[field].median()
    year_data["skew_islander"] = raw_data[field].skew()
    year_data["kurtosis_islander"] = raw_data[field].kurtosis()
    year_data["max_islander"] = raw_data[field].map(lambda x: int(x)).max()
    year_data["min_islander"] = raw_data[field].map(lambda x: int(x)).min()
    year_data["pct_10_islander"] = get_percentile(field, table, pct=.1, where=race_4_where)
    year_data["pct_20_islander"] = get_percentile(field, table, pct=.2, where=race_4_where)
    year_data["pct_30_islander"] = get_percentile(field, table, pct=.3, where=race_4_where)
    year_data["pct_40_islander"] = get_percentile(field, table, pct=.4, where=race_4_where)
    year_data["pct_50_islander"] = get_percentile(field, table, pct=.5, where=race_4_where)
    year_data["pct_60_islander"] = get_percentile(field, table, pct=.6, where=race_4_where)
    year_data["pct_70_islander"] = get_percentile(field, table, pct=.7, where=race_4_where)
    year_data["pct_80_islander"] = get_percentile(field, table, pct=.8, where=race_4_where)
    year_data["pct_90_islander"] = get_percentile(field, table, pct=.9, where=race_4_where)

    race_5_where = where + """ AND (race_1 = '5' OR race_2 = '5' OR race_3 = '5' OR race_4 = '5' OR race_5 = '5' OR
    co_race_1 = '5' OR co_race_2 = '5' OR co_race_3 = '5' OR co_race_4 = '5' OR co_race_5 = '5')"""
    raw_data = get_field_data(field, table,race_5_where)
    year_data["sum_white"] = get_field_sum(field, table, race_5_where)
    year_data["count_white"] = get_field_count(field, table, race_5_where)
    year_data["mean_white"] = get_field_mean(field, table, race_5_where)
    year_data["stdev_white"] = get_field_stdev(field, table, race_5_where)
    year_data["median_white"] = raw_data[field].median()
    year_data["skew_white"] = raw_data[field].skew()
    year_data["kurtosis_white"] = raw_data[field].kurtosis()
    year_data["max_white"] = raw_data[field].map(lambda x: int(x)).max()
    year_data["min_white"] = raw_data[field].map(lambda x: int(x)).min()
    year_data["pct_10_white"] = get_percentile(field, table, pct=.1, where=race_5_where)
    year_data["pct_20_white"] = get_percentile(field, table, pct=.2, where=race_5_where)
    year_data["pct_30_white"] = get_percentile(field, table, pct=.3, where=race_5_where)
    year_data["pct_40_white"] = get_percentile(field, table, pct=.4, where=race_5_where)
    year_data["pct_50_white"] = get_percentile(field, table, pct=.5, where=race_5_where)
    year_data["pct_60_white"] = get_percentile(field, table, pct=.6, where=race_5_where)
    year_data["pct_70_white"] = get_percentile(field, table, pct=.7, where=race_5_where)
    year_data["pct_80_white"] = get_percentile(field, table, pct=.8, where=race_5_where)
    year_data["pct_90_white"] = get_percentile(field, table, pct=.9, where=race_5_where)
    return year_data

def get_field_sum(field, table, where=None):
    sql = """SELECT SUM(CAST({field} AS INTEGER)) FROM {table} {where}""".format(field=field, table=table, where=where)
    cur = connect()
    cur.execute(sql,)
    sum_val = cur.fetchall()
    cur.close()
    return sum_val[0][0]

def get_field_count(field, table, where=None):
    sql = """SELECT COUNT({field}) FROM {table} {where}""".format(field=field, table=table, where=where)
    cur = connect()
    cur.execute(sql,)
    count_val = cur.fetchall()
    cur.close
    return count_val[0][0]

def get_field_mean(field, table, where=None):
    mean_sql = """SELECT ROUND(AVG(CAST({field} AS INTEGER)),2) FROM {table} {where} 
    ;""".format(field=field, table=table, where=where)
    #print(mean_sql)
    cur = connect()
    cur.execute(mean_sql)
    mean = cur.fetchall()
    mean = mean[0][0]
    cur.close()
    return mean

def get_field_stdev(field, table, where=None):
    stdev_sql = """SELECT ROUND(stddev_pop(CAST({field} AS INTEGER)),2) FROM {table} {where}
    ;""".format(field=field, table=table, where=where)
    cur = connect()
    cur.execute(stdev_sql)
    stdev = cur.fetchall()
    stdev = stdev[0][0]
    cur.close()
    return stdev

def get_percentile(field, table, pct, where=None):
    pct_sql = """SELECT
        percentile_cont({pct}) within group (order by CAST({field} AS INTEGER)) AS pctcont_10
        FROM {table} {where} ;""".format(field=field,table=table, pct=pct, where=where)
    cur = connect()
    cur.execute(pct_sql,)
    data=cur.fetchall()
    cur.close()
    return data[0][0]

def get_field_data(field, table, where=None):
    field_sql = """SELECT {field} FROM {table} {where} ;""".format(field=field, table=table, where=where)
    cur = connect()
    cur.execute(field_sql)
    data_df = pd.DataFrame(cur.fetchall(), columns=colnames)
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    return data_df

##get top level metrics
def get_count(field, table, where_clause=None):
    """"""
    sql = """SELECT COUNT({field}) FROM {table} """.format(field=field, table=table)
    if where_clause:
        sql = sql + where_clause
    cur = connect()
    #print(sql)
    cur.execute(sql,)
    data = cur.fetchall()
    cur.close()
    return data[0][0]

def get_sum(field, table, where_clause=None):
    """"""
    sql = """SELECT SUM(CAST({field} AS INTEGER)) FROM {table} """.format(field=field, table=table)
    if where_clause:
        sql = sql + where_clause
    cur = connect()
    #print(sql)
    cur.execute(sql,)
    data = cur.fetchall()
    cur.close()
    return data[0][0]


    def get_metrics(tables):
        """Calls metric assembling functions to produce a metrics dataframe. 
            Tables is the list of LAR tables from which to pull data."""
        metrics_list = []
        for table in tables:
            year = table[11:-6]
            year_dict = OrderedDict({})
            year_dict["year"] = year
            #assemble full dataframe row of data as dict and append to metrics_list
            #get count of total applications
            year_dict["app_count"] = get_count("*", table)
            #get count of total originations
            year_dict["orig_count"] = get_count("*", table, where_clause="WHERE action_taken_type = '1'")
            #get $ value of total applications
            year_dict["app_value"] = get_sum("amount", table)
            #get $ value of total originations
            year_dict["orig_value"] = get_sum("amount", table, where_clause="WHERE action_taken_type = '1'")
            #purchase count of apps/loans, $ value of apps/loans
            year_dict["purch_app_count"] = get_count("*", table, where_clause="WHERE purpose = '1'")
            year_dict["purch_orig_count"] = get_count("*", table, 
                                                      where_clause="WHERE purpose='1' AND action_taken_type='1'")

            year_dict["purch_app_value"] = get_sum("amount", table)
            year_dict["purch_orig_value"] = get_sum("amount", table, 
                                                    where_clause="WHERE purpose='1' AND action_taken_type='1'")
            #refinance count of apps/loans, $ value of apps/loans
            year_dict["refi_app_count"] = get_count("*", table, where_clause="WHERE purpose='3'")
            year_dict["refi_orig_count"] = get_count("*", table, 
                                                     where_clause="WHERE purpose='3' AND action_taken_type='1'")

            year_dict["refi_app_value"] = get_sum("amount", table, where_clause="WHERE purpose='3'")
            year_dict["refi_orig_value"] = get_sum("amount", table, 
                                                   where_clause="WHERE purpose='3' AND action_taken_type='1'")
            #improvement count of apps/loans, $ value of apps/loans
            year_dict["home_imp_app_count"] = get_count("*", table, where_clause="WHERE purpose='2'")
            year_dict["home_imp_orig_count"] = get_count("*", table, 
                                                        where_clause="WHERE purpose='2' AND action_taken_type='1'")

            year_dict["home_imp_app_value"] = get_sum("amount", table, where_clause="WHERE purpose='2'")
            year_dict["home_imp_orig_value"] = get_sum("amount", table, 
                                                       where_clause="WHERE purpose='2' AND action_taken_type='1'")
            #multifamily count of apps/loans, $ value of apps/loans
            year_dict["multifam_app_count"] = get_count("*", table, where_clause="WHERE property_type='3'")
            year_dict["multifam_orig_count"] = get_count("*", table, 
                                                        where_clause="WHERE property_type='3' AND action_taken_type='1'")
            year_dict["multifam_app_value"] = get_sum("amount", table, where_clause="WHERE property_type='3'")
            year_dict["multifam_orig_value"] = get_sum("amount", table, 
                                                       where_clause="WHERE property_type='3' AND action_taken_type='1'")
            #manufactured count of apps/loans, $ value of apps/loans
            year_dict["manu_app_count"] = get_count("*", table, where_clause="WHERE property_type='2'")
            year_dict["manu_orig_count"] = get_count("*", table, 
                                                     where_clause="WHERE property_type='2' AND action_taken_type='1'")
            year_dict["manu_app_value"] = get_sum("amount", table, where_clause="WHERE property_type='2'")
            year_dict["manu_orig_value"] = get_sum("amount", table, 
                                                   where_clause="WHERE property_type='2' AND action_taken_type='1'")
            #single family count of apps/loans, $ value of apps/loans
            year_dict["single_app_count"] = get_count("*", table, where_clause="WHERE property_type='1'")
            year_dict["single_orig_count"] = get_count("*", table, 
                                                    where_clause="WHERE property_type='1' AND action_taken_type='1'")
            year_dict["single_app_value"] = get_sum("amount", table, where_clause="WHERE property_type='1'")
            year_dict["single_orig_value"] = get_sum("amount", table, 
                                                     where_clause="WHERE property_type='1' AND action_taken_type='1'")
            #loan types: conventional, VA, FHA, RHS
            #conventional
            year_dict["conv_app_count"] = get_count("*", table, where_clause="WHERE loan_type='1'")
            year_dict["conv_orig_count"] = get_count("*", table,
                                                     where_clause="WHERE loan_type='1' AND action_taken_type='1'")
            year_dict["conv_app_value"] = get_sum("amount", table, where_clause="WHERE loan_type='1'")
            year_dict["conv_orig_value"] = get_sum("amount", table, 
                                                   where_clause="WHERE loan_type='1' AND action_taken_type='1'")
            #FHA
            year_dict["fha_app_count"] = get_count("*", table, where_clause="WHERE loan_type='2'")
            year_dict["fha_orig_count"] = get_count("*", table, 
                                                  where_clause="WHERE loan_type='2' AND action_taken_type='1'")
            year_dict["fha_app_value"] = get_sum("amount", table, where_clause="WHERE loan_type='2'")
            year_dict["fha_orig_value"] = get_sum("amount", table, 
                                                   where_clause="WHERE loan_type='2' AND action_taken_type='1'")
            #VA
            year_dict["va_app_count"] = get_count("*", table, where_clause="WHERE loan_type='3'")
            year_dict["va_orig_count"] = get_count("*", table, 
                                                    where_clause="WHERE loan_type='3' AND action_taken_type='1'")
            year_dict["va_app_value"] = get_sum("amount", table, where_clause="WHERE loan_type='3'")
            year_dict["va_orig_value"] = get_sum("amount", table, 
                                                  where_clause="WHERE loan_type='3' AND action_taken_type='1'")
            #RHS
            year_dict["rhs_app_count"] = get_count("*", table, where_clause="WHERE loan_type='4'")
            year_dict["rhs_orig_count"] = get_count("*", table, 
                                                  where_clause="WHERE loan_type='4' AND action_taken_type='1'")
            year_dict["rhs_app_value"] = get_sum("amount", table, where_clause="WHERE loan_type='4'")
            year_dict["rhs_orig_value"] = get_sum("amount", table, 
                                                   where_clause="WHERE loan_type='4' AND action_taken_type='1'")
            metrics_list.append(year_dict)
        return metrics_list

def get_frb_metrics(tables):
    """Calls metric assembling functions to produce tables from the FRB bulletin. 
    Tables is the list of LAR tables from which to pull data."""
    metrics_list = []
    for table in tables:
        year = table[11:-6]
        year_dict = OrderedDict({})
        year_dict["year"] = year

        #Purchase
        year_dict["purch_app_count"] = get_count("*", table, where_clause="WHERE purpose='1'")
        year_dict["purch_orig_count"] = get_count("*", table, 
                                                  where_clause="WHERE purpose='1' AND action_taken_type='1'")
        #Purchase, first lien, owner occ
        year_dict["purch_orig_1st_occ"] = get_count("*", table, 
            where_clause="WHERE purpose='1' AND action_taken_type='1' AND lien_status='1' AND occupancy='1'")
        #Purchase, first lien, owner occ, site built, conventional
        year_dict["purch_orig_1st_occ_site_conv"] = get_count("*", table, 
            where_clause="""WHERE purpose='1' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='1' AND property_type='1'""")
        #Purchase, first lien, owner occ, site built, non-conv
        year_dict["purch_orig_1st_occ_site_nonconv"] = get_count("*", table, 
            where_clause="""WHERE purpose='1' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type IN ('2', '3', '4') AND property_type='1' """)
        #Purchase, first lien, owner occ, site built, non-conv: share FHA
        year_dict["purch_share_fha"] = round(get_count("*", table, 
            where_clause="""WHERE purpose='1' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='2' AND property_type='1' """) /  year_dict["purch_orig_1st_occ_site_nonconv"], 4) *100
        #Purchase, first lien, owner occ, site built, non-conv: share VA
        year_dict["purch_share_va"] = round(get_count("*", table, 
            where_clause="""WHERE purpose='1' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='3' AND property_type='1' """) /  year_dict["purch_orig_1st_occ_site_nonconv"], 4) *100
        #Purchase, first lien, owner occ, site built, non-conv: share RHS
        year_dict["purch_share_rhs"] = round(get_count("*", table, 
            where_clause="""WHERE purpose='1' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='4' AND property_type='1' """) /  year_dict["purch_orig_1st_occ_site_nonconv"], 4) *100

        #manufactured, conventional
        year_dict["purch_orig_manu_conv"] = get_count("*", table, where_clause="""WHERE purpose='1' AND
        action_taken_type='1' AND property_type='2' AND loan_type = '1' AND lien_status='1' AND occupancy='1'""")
        #manufactured, nonconventional
        year_dict["purch_orig_manu_nonconv"] = get_count("*", table, where_clause="""WHERE purpose='1' AND
        action_taken_type='1' AND property_type='2' AND loan_type IN ('2', '3', '4') 
        AND lien_status='1' AND occupancy='1'""")

        #first lien, non-owner occ
        year_dict["purch_1st_non_owner"] = get_count("*", table, where_clause="""WHERE purpose='1' AND
        lien_status='1' AND occupancy !='1'""")
        #junior lien, owner occ
        year_dict["purch_sub_owner"] = get_count("*", table, where_clause="""WHERE purpose='1' AND
        lien_status !='1' AND occupancy ='1'""")
        #junior lien, non-owner occ
        year_dict["purch_sub_non_owner"] = get_count("*", table, where_clause="""WHERE purpose='1' AND
        lien_status!='1' AND occupancy!='1'""")
        #refinance
        year_dict["refi_app_count"] = get_count("*", table, where_clause="WHERE purpose='3'")
        year_dict["refi_orig_count"] = get_count("*", table, 
                                                  where_clause="WHERE purpose='3' AND action_taken_type='1'")
        #refinance, first lien, owner occ
        year_dict["refi_orig_1st_occ"] = get_count("*", table, 
        where_clause="WHERE purpose='3' AND action_taken_type='1' AND lien_status='1' AND occupancy='1'")
        #refinance, first lien, owner occ, site built, conventional
        year_dict["refi_orig_1st_occ_site_conv"] = get_count("*", table, 
            where_clause="""WHERE purpose='3' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='1' AND property_type='1'""")
        #refinance, first lien, owner occ, site built, non-conventional
        year_dict["refi_orig_1st_occ_site_nonconv"] = get_count("*", table, 
            where_clause="""WHERE purpose='3' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type IN ('2', '3', '4') AND property_type='1' """)
        #Share FHA
        year_dict["refi_share_fha"] = round(get_count("*", table, 
            where_clause="""WHERE purpose='3' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='2' AND property_type='1' """) /  year_dict["refi_orig_1st_occ_site_nonconv"], 4) *100
        #Share VA
        year_dict["refi_share_va"] = round(get_count("*", table, 
            where_clause="""WHERE purpose='3' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='3' AND property_type='1' """) /  year_dict["refi_orig_1st_occ_site_nonconv"], 4) *100
        #Share RHS
        year_dict["refi_share_rhs"] = round(get_count("*", table, 
            where_clause="""WHERE purpose='3' AND action_taken_type='1' AND lien_status='1' AND occupancy='1' 
            AND loan_type='4' AND property_type='1' """) /  year_dict["refi_orig_1st_occ_site_nonconv"], 4) *100

        #manufactured, conventional
        year_dict["refi_orig_manu_conv"] = get_count("*", table, where_clause="""WHERE purpose='3' AND
        action_taken_type='1' AND property_type='2' AND loan_type = '1' AND lien_status='1' AND occupancy='1'""")
        #manufactured, nonconventional
        year_dict["refi_orig_manu_nonconv"] = get_count("*", table, where_clause="""WHERE purpose='3' AND
        action_taken_type='1' AND property_type='2' AND loan_type IN ('2', '3', '4') 
        AND lien_status='1' AND occupancy='1'""")
        #first lien, non-owner occ
        year_dict["purch_1st_non_owner"] = get_count("*", table, where_clause="""WHERE purpose='3' AND
        lien_status='1' AND occupancy !='1'""")
        #junior lien, owner occ
        year_dict["purch_sub_owner"] = get_count("*", table, where_clause="""WHERE purpose='3' AND
        lien_status !='1' AND occupancy ='1'""")
        #junior lien, non-owner, occ
        year_dict["purch_sub_non_owner"] = get_count("*", table, where_clause="""WHERE purpose='3' AND
        lien_status!='1' AND occupancy!='1'""")

        #home improvment apps, orig
        year_dict["home_imp_app_count"] = get_count("*", table, where_clause="""WHERE purpose='2'""")
        year_dict["home_imp_orig_count"] = get_count("*", table, where_clause="""WHERE purpose='2' AND
        action_taken_type='1' """)
        #multifamily apps, orig
        year_dict["multi_app_count"] = get_count("*", table, where_clause="""WHERE property_type='3'""")
        year_dict["multi_orig_count"] = get_count("*", table, where_clause="""WHERE property_type='3' AND 
        action_taken_type = '1'""")
        #total apps, orig
        year_dict["total_app_count"] = get_count("*", table)
        year_dict["total_orig_count"] = get_count("*", table, where_clause="""WHERE action_taken_type='1'""")
        #purchased loans
        year_dict["purchased_count"] = get_count("*", table, where_clause="""WHERE action_taken_type='6'""")
        #preapproval requests
        year_dict["preapproval_req_count"] = get_count("*", table, where_clause="""WHERE preapprovals='1'""")
        #preapproval requests approved but not acted upon
        year_dict["preapproval_nonaction_count"] = get_count("*", table, where_clause="""WHERE preapprovals='1' AND
        action_taken_type='8'""")
        #preapproval requests denied
        year_dict["preapproval_denied_count"] = get_count("*", table, where_clause="""WHERE preapprovals='1' AND
        action_taken_type='7'""")
        metrics_list.append(year_dict)
    return metrics_list




