from sutime import SUTime
import re
import json
from datetime import datetime,timedelta,tzinfo
from dateutil.relativedelta import relativedelta

sutime = SUTime(mark_time_ranges=False, include_range=False)
date_regex = re.compile(r"\d{3,4}\s*-\s*\d+\s*-\s*\d+")

def parse_date(date):
    year = -1
    month = -1
    day = -1
    items = date.split("-")
    if items[0] == "":
        items = items[1:]
        items[0] = "-" + items[0]
    year = items[0]
    if "XX" in year:
        start_year = int(year[:-2]) * 100
        end_year = start_year + 100
        return [start_year,end_year],month,day
    elif "X" in year:
        start_year = int(year[:-1]) * 10
        end_year = start_year + 10
        return [start_year,end_year],month,day
    else:
        year = int(year)
    if len(items) > 1:
        month = int(items[1])
    if len(items) > 2:
        day = int(items[2])
    return year,month,day

def parse_timex_to_interval(timex_val):
    date = timex_val.split("T")[0]
    year,month,day = parse_date(date)
    if type(year) != type(int(1)):
        start_time = datetime(year[0],1,1,0,0,0)
        end_time = datetime(year[1],1,1,0,0,0)
    else:
        if month == -1:
            start_time = datetime(year,1,1,0,0,0)
            end_time = start_time + relativedelta(years=1)
        else:
            if day == -1:
                start_time = datetime(year,month,1,0,0,0)
                end_time = start_time + relativedelta(months=1)
            else:
                start_time = datetime(year,month,day,0,0,0)
                end_time = start_time + timedelta(days=1)
    start_time_literal = "\"{}Z\"^^xsd:dateTime".format(start_time.isoformat())
    end_time_literal = "\"{}Z\"^^xsd:dateTime".format(end_time.isoformat())
    return [start_time_literal,end_time_literal]

def annotate_datetime(text,ref_date=''):
    try:
        parse_results = sutime.parse(text,reference_date=ref_date)
    except:
        parse_results = []
    it = date_regex.finditer(text)
    for date_match in it:
        date_expr = date_match.group()
        year,month,day = [item.strip() for item in date_expr.split("-")]
        if month == "0" and day == "0":
            timex = year
        elif day == "0":
            timex = year + "-" + month
        else:
            timex = date_expr
        start_index = text.find(date_expr)
        end_index = start_index + len(date_expr)
        annotate = {"timex-value":timex,"type":"DATE","start":start_index,"end":end_index,"text":date_expr}
        parse_results.append(annotate)

    time_annotates = []
    for parse_result in parse_results:
        if "the year of" in parse_result["text"]:
            year = re.findall(r"\d+",parse_result["text"])[0]
            parse_result["timex-value"] = year
        try:
            interval = parse_timex_to_interval(parse_result["timex-value"])
        except:
            interval = None
        if interval is not None:
            parse_result["interval"] = interval
            time_annotates.append(parse_result)
    return time_annotates
