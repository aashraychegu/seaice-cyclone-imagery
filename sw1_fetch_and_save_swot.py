
import argparse
from pathlib import Path
import datetime as dt

import earthaccess


parser = argparse.ArgumentParser(
    description="Download SWOT L2 LR SSH Basic granules from PO.DAAC using earthaccess (netrc auth)."
)
parser.add_argument("--short-name", default="SWOT_L2_LR_SSH_Basic_2.0")
parser.add_argument("--out-dir", type=Path, default=Path("intermediates/shapes/swot"))
parser.add_argument("--start", default="2014-01-01")
parser.add_argument("--end", default=dt.datetime.now(dt.UTC).strftime("%Y-%m-%d"))
parser.add_argument("--bbox", type=float, nargs=4, metavar=("W", "S", "E", "N"), default=(-180.0, -80.0, 180.0, -60.0))
args = parser.parse_args()

args.out_dir.mkdir(parents=True, exist_ok=True)

earthaccess.login(strategy="netrc")

granules = earthaccess.search_data(
    short_name=args.short_name,
    temporal=(args.start, args.end),
    bounding_box=tuple(args.bbox),
)

print(f"Granules found: {len(granules)}")
downloaded = earthaccess.download(granules, str(args.out_dir),show_progress = True, threads = 32)
