import pandas as pd
import numpy as np
import datetime as dt
import json
import re
from datetime import datetime
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 20)
pd.set_option('expand_frame_repr', False)


def get_data_from_query(filename):
    with open(filename, 'r') as myfile:
        data = myfile.read()

    data_obj = json.loads(data)
    return data_obj["data"]["courses"]["feed"]


def parse_data_from_query(courses):
    df = pd.DataFrame(
        columns=["id", "name", "credits_min", "credits_max", "period", 'Summer', 'I', 'II', 'III', 'IV',
                 'V', 'I-II', 'I-III', 'I-IV', 'I-V', 'II-III', 'II-IV', 'II-V', 'III-IV', 'III-V', 'IV-V', 'V-I'])
    # Parse json to df
    for course in courses:
        period_ = course['details']['summary']['teachingPeriod']['fi']
        new_row = {'id': re.sub(r"\s+", "", course['id'], flags=re.UNICODE), 'name': course['details']['name']['fi'], 'credits_min': course['details']
                   ['credits']['min'], 'credits_max': course['details']['credits']['max'], 'period': period_}

        df = df.append(new_row, ignore_index=True)
    return df


#re.sub(r"\s+", "", course['id'], flags=re.UNICODE)


def parse_periods_from_query(df):
    df_p = df.copy()
    possible_periods = ['-', 'Summer', 'I', 'II', 'III', 'IV',
                        'V', 'I-II', 'I-III', 'I-IV', 'I-V', 'II-III', 'II-IV', 'II-V', 'III-IV', 'III-V', 'IV-V', 'V-I']
    #############################################
    p_split = df_p["period"].str.split(
        r",|\(|\)|\s+", expand=True).applymap(lambda x: x.strip() if isinstance(x, str) else x)
    p_split = p_split.applymap(
        lambda x: None if x not in possible_periods else x)
    #############################################
    for i in range(len(df_p)):
        periods_test = list()
        curr_row = p_split.iloc[i].dropna().to_list()
        if "-" in curr_row:
            periods_test = [curr_row[j-1]+"-"+curr_row[j+1]
                            for j in range(len(curr_row)) if curr_row[j] == "-"]
        else:
            periods_test = curr_row
        for period_to_find in possible_periods:

            if period_to_find in periods_test:
                df_p.loc[i, period_to_find] = 1
            else:
                df_p.loc[i, period_to_find] = 0
    df_p = df_p.drop(columns=["-"])
    return df_p


def query_pipeline():
    courses = get_data_from_query(r'data\course_json_source1.json')
    df_query = parse_data_from_query(courses)
    parsed = parse_periods_from_query(df_query)
    return parsed

# For weboodi:


def get_data_from_oodi(filename):
    with open(filename, 'r') as myfile:
        data = myfile.read()
    data_obj = json.loads(data)
    return data_obj


def parse_oodi(json_obj):
    courses = json_obj
    res = pd.DataFrame(
        columns=["id", "name", "credits_min", "credits_max", "period", 'Summer', 'I', 'II', 'III', 'IV',
                 'V', 'I-II', 'I-III', 'I-IV', 'I-V', 'II-III', 'II-IV', 'II-V', 'III-IV', 'III-V', 'IV-V', 'V-I'])
    periods_by_week = pd.DataFrame(data=[['I', 37, 43], ['II', 44, 50], ['III', 2, 8], ['IV', 9, 15], ['V', 16, 22], ['Summer', 23, 36]],
                                   columns=["period", 'week_min', 'week_max'])
    possible_periods = ['Summer', 'I', 'II', 'III', 'IV',
                        'V', 'I-II', 'I-III', 'I-IV', 'I-V', 'II-III', 'II-IV', 'II-V', 'III-IV', 'III-V', 'IV-V', 'V-I']

    for i in range(len(courses)):

        course_id = courses[i]['opintokohde']['opintokohteenTunniste']
        name = courses[i]['opintokohde']['opintokohteenNimi']
        credits_min = courses[i]['opintokohde']['laajuusOp']
        credits_max = courses[i]['opintokohde']['maksimilaajuusOp']

        new_row = {'id': course_id, 'name': name,
                   'credits_min': credits_min, 'credits_max': credits_max}
        res = res.append(new_row, ignore_index=True)

        pvms = courses[i]['opetustapahtumat']
        periods = []
        for j in range(len(pvms)):
            if pvms[j]['opetustapahtumanTyyppiSelite'] == "Kurssi":
                #                 sdate = datetime.fromtimestamp(pvms[j]['alkuPvm']/1000).strftime('%Y-%m-%d')
                #                 edate = datetime.fromtimestamp(pvms[j]['loppuPvm']/1000).strftime('%Y-%m-%d')
                sweek = int(datetime.fromtimestamp(
                    pvms[j]['alkuPvm']/1000).strftime("%V"))
                eweek = int(datetime.fromtimestamp(
                    pvms[j]['loppuPvm']/1000).strftime("%V"))
                sweek = min(periods_by_week["week_min"],
                            key=lambda x: abs(x-sweek))
                eweek = min(periods_by_week["week_max"],
                            key=lambda x: abs(x-eweek))
                speriod = periods_by_week.loc[(
                    periods_by_week["week_min"] == sweek), "period"].item()
                eperiod = periods_by_week.loc[(
                    periods_by_week["week_max"] == eweek), "period"].item()
                if speriod == eperiod:
                    period = speriod
                else:
                    period = speriod + "-" + eperiod

                periods.append(period)

        res.loc[i, "period"] = ",".join(periods)
        for period_to_find in possible_periods:
            if period_to_find in periods:
                res.loc[i, period_to_find] = 1
            else:
                res.loc[i, period_to_find] = 0
    return res


def oodi_pipeline():
    courses = get_data_from_oodi(r'data\course_json_source2.json')
    parsed = parse_oodi(courses)
    return parsed


def get_final_data():
    query_df = query_pipeline()
    oodi_df = oodi_pipeline()
    res = query_df.append(oodi_df, ignore_index=True)
    print("{} duplicates from two data sources.".format(
        sum(res.duplicated(subset=["id"], keep='first'))))
    return res


def main():
    print(get_final_data())


if __name__ == '__main__':
    main()
