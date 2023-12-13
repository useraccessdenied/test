path = "/data"
redis_host = "localhost"
redis_port = 6379

import redis
import pandas as pd
from datetime import datetime, timedelta
from zipfile import ZipFile
import io
import json
import logging as log
import traceback
import glob

log.basicConfig(level=log.NOTSET, format="%(asctime)s - %(filename)s - %(levelname)s: %(message)s")

try:
    log.info("Script for BSE started")
    redis.cluster.RedisCluster()
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    lastknownstocks = r.hgetall("lastKnownStockValue")

    for k in lastknownstocks:
        t = json.loads(lastknownstocks[k])
        lastknownstocks[k] = t

    one_day = timedelta(1)
    date_today = datetime.now() - one_day

    file_date_str = datetime.strftime(date_today, "%d%m%y").upper()
    # url = f"{path}/EQ{file_date_str}_CSV.ZIP"

    # Get file with wildcard support
    file_list = glob.glob(f"{path}/EQ*")
    if file_list:
        url = file_list[0]
        log.info(f"Using file {url}")
    else:
        raise FileNotFoundError("File for BSE not found")

    bse = pd.read_csv(url, dtype=str, compression="zip")
    bse["TOKEN"] = "1.1!" + bse["SC_CODE"]
    bse["CLOSE"] = pd.to_numeric(bse["CLOSE"])
    bse["OPEN"] = pd.to_numeric(bse["OPEN"])
    bse["HIGH"] = pd.to_numeric(bse["HIGH"])
    bse["LOW"] = pd.to_numeric(bse["LOW"])
    bse["PREVCLOSE"] = pd.to_numeric(bse["PREVCLOSE"])
    bse["LAST"] = pd.to_numeric(bse["LAST"])
    bse["PER_CHANGE"] = ((bse["LAST"] - bse["PREVCLOSE"]) / bse["PREVCLOSE"]) * 100
    bse["PER_CHANGE"] = bse["PER_CHANGE"].round(2)
    bse_dict = (
        bse[["TOKEN", "OPEN", "LAST", "HIGH", "LOW", "PREVCLOSE", "PER_CHANGE"]]
        .set_index("TOKEN")
        .T.to_dict()
    )

    missed = []

    for k in lastknownstocks:
        if k.startswith("1.1"):
            if k in bse_dict:
                lastknownstocks[k][20] = bse_dict[k]["PREVCLOSE"]
                lastknownstocks[k][5] = bse_dict[k]["PER_CHANGE"]
                lastknownstocks[k][1] = bse_dict[k]["OPEN"]
                lastknownstocks[k][2] = bse_dict[k]["LAST"]
                lastknownstocks[k][3] = bse_dict[k]["HIGH"]
                lastknownstocks[k][4] = bse_dict[k]["LOW"]
            else:
                missed.append(k)

    log.warn(f"Total {len(missed)} have missed tokens in bhavcopy.")
    log.warn(missed)

    for k in lastknownstocks:
        t = json.dumps(lastknownstocks[k])
        lastknownstocks[k] = t

    r.hset("lastKnownStockValue", mapping=lastknownstocks)

    log.info("Script for BSE succeeded.")
except Exception:
    log.error(traceback.format_exc())
