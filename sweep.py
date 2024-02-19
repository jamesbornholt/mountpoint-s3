"""
python3 sweep.py --part-size 512K 1M 2M 4M 8M --connections-per-vip 10 20 30 40 50 -- target/release/examples/download_crt --region us-west-2 --maximum-throughput-gbps 200 --iterations 20 bornholt-test--usw2-az3--x-s3 test20GiB
"""


import argparse
import re
import subprocess
import sys
from typing import List

THROUGHPUT_RE = re.compile(r" (\d+\.\d+)MiB/s")

def run_once(cmd: List[str], part_size: int, connections_per_vip: int) -> float:
    cmd = cmd + ["--part-size", str(part_size), "--connections-per-vip", str(connections_per_vip)]
    out = subprocess.check_output(cmd).decode("utf-8")
    m = THROUGHPUT_RE.findall(out)
    if not m:
        raise Exception("No throughput found")
    throughput = max(float(x) for x in m)
    return throughput

def expand_size(size: str) -> int:
    if size.endswith("M"):
        return int(size[:-1]) * 1024 * 1024
    elif size.endswith("K"):
        return int(size[:-1]) * 1024
    else:
        return int(size)

def run_sweep(args: argparse.Namespace, cmd: List[str]):
    print(f"part size\tconnections per vip\tmax throughput")
    for part_size in args.part_size:
        for connections_per_vip in args.connections_per_vip:
            sys.stdout.write(f"{part_size}\t{connections_per_vip}\t")
            sys.stdout.flush()
            try:
                throughput = run_once(cmd, expand_size(part_size), connections_per_vip)
                print(throughput)
            except Exception as e:
                print(f"failed ({e})")
            

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--part-size", nargs="+")
    p.add_argument("--connections-per-vip", type=int, nargs="+")
    args, cmd = p.parse_known_args()
    if cmd[0] != "--":
        raise Exception("missing remaining args")

    run_sweep(args, cmd[1:])

if __name__ == "__main__":
    main()
