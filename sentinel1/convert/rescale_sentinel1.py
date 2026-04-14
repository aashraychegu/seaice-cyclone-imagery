import asyncio
from pathlib import Path
from tqdm.asyncio import tqdm
import argparse

async def run_command(command):
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()

async def main():
    parser = argparse.ArgumentParser(description="Downscales geotiffs")
    parser.add_argument("--in-tiffs", default="./intermediates/tiffs")
    parser.add_argument("--out-tiffs", default="./intermediates/downsampled_tiffs")
    parser.add_argument("--xres", default=1000, type=int)
    parser.add_argument("--yres", default=1000, type=int)
    args = parser.parse_args()

    in_stem = Path(args.in_tiffs)
    out_stem = Path(args.out_tiffs)
    files = list(in_stem.glob("**/*.tiff"))

    commands = []
    for file in tqdm(files):
        (out_stem / file.parent.name).mkdir(exist_ok=True)
        commands.append([
            "gdalwarp", "-overwrite", "-ts",
            str(args.xres), str(args.yres),
            str(file), str(out_stem / file.parent.name / file.name)
        ])

    # Run commands asynchronously with a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(32)  # Limit to 32 concurrent processes

    async def limited_run(command):
        async with semaphore:
            await run_command(command)

    tasks = [limited_run(cmd) for cmd in commands]
    await tqdm.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())