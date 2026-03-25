import subprocess
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--in_data_dir", default="./intermediates/era5_mslp")
parser.add_argument("--TE_temps", default = "./intermediates/tempestextreme_files/")
parser.add_argument("--out_data_dir", default="./intermediates/detectnodes/")
args = parser.parse_args()

in_data_dir = Path(args.in_data_dir)
out_data_dir = Path(args.out_data_dir)
tetf_dir = Path(args.TE_temps)

detectnodes_in = tetf_dir / "detectnodes-in.txt"
detectnodes_in.unlink(missing_ok=True)
detectnodes_in.touch()

detectnodes_out = tetf_dir / "detectnodes-out.txt"
detectnodes_out.unlink(missing_ok=True)
detectnodes_out.touch()

in_data_file_paths = in_data_dir.glob(f"*.nc")

in_data_files = [str(path.resolve()) for path in in_data_file_paths]
in_data_files.sort()

filenames = [path.stem for path in in_data_file_paths]

out_data_files = [out_data_dir / f"{name}.txt" for name in filenames]


detectnodes_in.write_text("\n".join(in_data_files))
detectnodes_out.write_text("\n".join(out_data_files))

def add_flag(command: list[str], flag: str, arg: str = None):
    """Add a flag and optional argument to the command list."""
    command.append(flag)
    if arg is not None:
        command.append(arg)


command = ["DetectNodes"]
add_flag(command, "--in_data_list", str(detectnodes_in))
add_flag(command, "--out_data_list", str(detectnodes_out))
add_flag(command, "--searchbymin", "msl")  # Changed from mslp_snapshot to msl
add_flag(command, "--closedcontourcmd", "msl,200.0,4.0,0")  # Changed variable name
add_flag(command, "--mergedist", "6.0")
add_flag(command, "--outputcmd", "msl,min,0")  # Changed variable name
add_flag(command, "--latname", "latitude")  # Changed from lat to latitude
add_flag(command, "--lonname", "longitude")  # Changed from lon to longitude
add_flag(command, "--timefilter", "6hr")
add_flag(command, "--out_header")

subprocess.run(command)