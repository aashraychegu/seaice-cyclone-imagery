import cdsapi
from pathlib import Path
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(prog="downloads netcdf files for tempestextremes")
parser.add_argument("--dir", default = "./intermediates/era5_mslp")
parser.add_argument("--startyr",default = 2014)
parser.add_argument("--endyr", default = 2024)

args = parser.parse_args()

mslpdir = Path(args.dir)
yr_to_path = {str(yr):mslpdir/f"{str(yr)}_mslp.nc" for yr in range(args.startyr,args.endyr+1)}

assert mslpdir.exists(), f"{mslpdir} doesn't exist"

def build_request(yr,path):
    dataset = "reanalysis-era5-single-levels"
    request = {
        "product_type": ["reanalysis"],
        "variable": ["mean_sea_level_pressure"],
        "year": [yr],
        "month": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12"
        ],
        "day": [
            "01", "02", "03",
            "04", "05", "06",
            "07", "08", "09",
            "10", "11", "12",
            "13", "14", "15",
            "16", "17", "18",
            "19", "20", "21",
            "22", "23", "24",
            "25", "26", "27",
            "28", "29", "30",
            "31"
        ],
        "time": [
            "00:00", "06:00", "12:00",
            "18:00"
        ],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": [-60, -180, -90, 180]
    }
    return {"dataset":dataset,"request":request,"path":path}

client = cdsapi.Client()

for yr,path in tqdm(yr_to_path.items()):
    print(f"Downloading {yr} to {path}")
    request_info = build_request(yr,path)
    client.retrieve(request_info["dataset"], request_info["request"]).download(request_info["path"])
