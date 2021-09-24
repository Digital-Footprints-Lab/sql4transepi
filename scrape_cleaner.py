#~ Standard library imports
import sys
import os
import re
import signal
import argparse
import csv

#~ 3rd party imports
import pandas as pd


def args_setup():

    parser = argparse.ArgumentParser(
        description="Scrape Cleaner",
        epilog="Example: python scrape_cleaner.py -i input_file.csv -o output_file.csv --field 3")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="The name of the input CSV file to process.")
    parser.add_argument(
        "-o", "--output", type=argparse.FileType("w"), default=sys.stdin,
        metavar="PATH", required=True,
        help="The name of the saved output file to create.")
    parser.add_argument(
        "--field", type=str, action="store",
        help="The field (column) name to process.")

    args = parser.parse_args()

    return parser, args


def cleaner(dirty):

    """
    Cleans
    """

    return re.sub(r"(?<=[.,:;])(?=[^\s])", r" ", dirty)


def main():

    parser, args = args_setup()

    if len(sys.argv) < 6:
        parser.print_help(sys.stderr)
        print(f"\n!!! Your clean request was incomplete, see above for help.")
        sys.exit(1)

    df = pd.read_csv(args.input)

    df[args.field] = df[args.field].apply(cleaner)

    df.to_csv(args.output, index=True)


if __name__ == "__main__":

    main()

