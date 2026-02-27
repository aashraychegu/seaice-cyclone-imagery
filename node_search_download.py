import pycurl
import polars as pl
import argparse
from io import BytesIO
import json

from tqdm import tqdm

parser = argparse.ArgumentParser(
        description="Download Sentinel-1 TIFF files from S3 based on CSV"
    )
parser.add_argument(
    "--csv",
    required=True,
    help="Path to CSV file with 'id' and 's3_path' columns"
)
parser.add_argument(
    "--output",
    required=True,
    help="Output directory where folders with ID names will be created"
)
parser.add_argument(
    "--tokens",
    default=".env",
    help=""
)
parser.add_argument(
    "--workers",
    type=int,
    default=4,
    help="Number of parallel workers (default: 4)"
)

args = parser.parse_args()
df = pl.read_csv(args.csv)

pair = df[1]

download_urls = []
for pair in tqdm(df.iter_rows()):
    cid = pair["id"].item()
    s3path = pair["s3_path"].item()

    archive_name = s3path.split("/")[-1]

    search_url = (f"https://download.dataspace.copernicus.eu/odata/v1/"
                f"Products({cid})/"
                f"Nodes({archive_name})/Nodes(measurement)/Nodes")

    c = pycurl.Curl()
    c.setopt(c.URL, search_url)
    buffer = BytesIO()
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    assert c.getinfo(pycurl.HTTP_CODE) == 200, "oops"
    c.close()
    result = json.loads(buffer.getvalue().decode('utf-8'))["result"]

    construct_download_url = lambda tiff_id: search_url + f"Nodes({tiff_id})$value"

    for i in result:
        filename = i["Id"]
        download_information = {
            "download_url":construct_download_url(filename),
            "folder":cid,
            "filename":filename
        }

download_dataframe = pl.DataFrame(download_information)
print(download_dataframe)