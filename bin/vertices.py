import argparse
import polars as pl

parser = argparse.ArgumentParser(
    description="Visualize the reconstructed annihilation vertices for a single run"
)
parser.add_argument("infile", help="path to the reconstructed vertices CSV file")
args = parser.parse_args()

df = pl.read_csv(args.infile, comment_prefix="#")
