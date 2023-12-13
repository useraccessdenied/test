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
    log.info("Script for NSE started")
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    lastknownstocks = r.hgetall("lastKnownStockValue")

    for k in lastknownstocks:
        t = json.loads(lastknownstocks[k])
        lastknownstocks[k] = t

    one_day = timedelta(1)
    date_today = datetime.now() - one_day

    file_date_str = datetime.strftime(date_today, "%d%b%Y").upper()
    file_year_str = datetime.strftime(date_today, "%Y")
    file_month_str = datetime.strftime(date_today, "%b").upper()

    # url = f"{path}/cm{file_date_str}bhav.csv.zip"

    # Get file with wildcard support
    file_list = glob.glob(f"{path}/cm*bhav*")
    if file_list:
        url = file_list[0]
        log.info(f"Using file {url}")
    else:
        raise FileNotFoundError("File for NSE not found")

    nse = pd.read_csv(url, dtype=str, compression="zip")
    nse_token_mapper = pd.read_csv(
        f"{path}/NSEScripMaster.txt",
        dtype=str,
    )

    nse_token_mapper.columns = nse_token_mapper.columns.str.replace('"', "")
    nse_token_mapper.columns = nse_token_mapper.columns.str.strip()

    nse_token_mapper["SYMBOL"] = nse_token_mapper["ExchangeCode"]
    nse_token_mapper2 = nse_token_mapper[["Token", "SYMBOL"]]
    nse.drop("Token", axis=1, inplace=True, errors="ignore")
    nse = pd.merge(
        left=nse, right=nse_token_mapper[["Token", "SYMBOL"]], on="SYMBOL", how="left"
    )

    missing_tokens = nse[nse["Token"].isnull()]
    log.warn(f"Total {len(missing_tokens)} entries have missing tokens")
    log.warn(missing_tokens)

    nse = nse[nse["Token"].notnull()]
    nse["iToken"] = "4.1!" + nse["Token"]
    nse["CLOSE"] = pd.to_numeric(nse["CLOSE"])
    nse["PREVCLOSE"] = pd.to_numeric(nse["PREVCLOSE"])
    nse["LAST"] = pd.to_numeric(nse["LAST"])
    nse["PER_CHANGE"] = ((nse["LAST"] - nse["PREVCLOSE"]) / nse["PREVCLOSE"]) * 100
    nse["PER_CHANGE"] = nse["PER_CHANGE"].round(2)
    nse_dict = (
        nse[["iToken", "LAST", "CLOSE", "PREVCLOSE", "PER_CHANGE"]]
        .set_index("iToken")
        .T.to_dict()
    )

    missed = 0

    for k in lastknownstocks:
        if k.startswith("4.1"):
            if k in nse_dict:
                lastknownstocks[k][20] = nse_dict[k]["PREVCLOSE"]
                lastknownstocks[k][5] = nse_dict[k]["PER_CHANGE"]
                lastknownstocks[k][2] = nse_dict[k]["LAST"]
            else:
                missed += 1

    # log.info(f"Missed script {missed}")

    for k in lastknownstocks:
        t = json.dumps(lastknownstocks[k])
        lastknownstocks[k] = t

    r.hset("lastKnownStockValue", mapping=lastknownstocks)

    log.info("Script for NSE succeeded.")
except Exception:
    log.error(traceback.format_exc())
