import json
import csv
import argparse
from collections import OrderedDict
import sys
import hashlib


def args_setup():

    parser = argparse.ArgumentParser(
        description="JSON to CSV converter: Tesco's Clubcards.",
        epilog="Example: python JSON2CSV.py -i clubcard_42.json")
    parser.add_argument(
        "-i", "--input", type=argparse.FileType("r"), default=sys.stdin,
        metavar="PATH", required=True,
        help="JSON file to convert to CSV.")

    args = parser.parse_args()

    return parser, args

parser, args = args_setup()


def generate_hash_field(hash_input):

    """
    Args:       Some text to be hashed

    Returns:    A unique 16 char hex identifier.
    """

    _hash = hashlib.sha1(str(hash_input).encode())
    _hash_hex = _hash.hexdigest()[0:16]

    return _hash_hex


def json_items_to_csv_file(json_file):

    """
    Does the JSON nest-diving to create a single row per item.
    In the JSON, each item does not have useful things like timestamps,
    customerId, etc, so those are extracted from upper nests and
    reinserted for each product.

    Args:       incoming JSON file
                hex identifier from generate_hash_id()
    Rets:       Nothing, outputs a csv file names after the incoming json
    """

    print(f"Converting transaction items to CSV rows.")

    items = []

    with open(json_file) as json_infile:
        data = json.load(json_infile)

    #~ Tesco card JSONs do not have a unique and persistent user identifier:
    #~ Here we assign one by hashing their 1st transaction metadata,
    #~ which should be unique and immutable
    transaction1_store_id = data["Purchase"][0][0]["storeId"]
    transaction1_timestamp = data["Purchase"][0][0]["timeStamp"]
    customerId = generate_hash_field(transaction1_store_id + transaction1_timestamp)
    print(f"\nAssigning hash as customer identifier: {customerId}")

    #~ The "Purchase" nest is a >>list<< of transactions
    for transaction in data["Purchase"][0]:
        storeId = transaction["storeId"]
        timeStamp = transaction["timeStamp"]
        #~ generate basket identifier hash
        basketId = generate_hash_field(storeId + timeStamp)
        #~ The "product" nest is a >>dict<< of items in the transaction
        for item in transaction["product"]:
            _this_item = {}
            #~ add extra fields, from hashes and containing nest
            _this_item["customerId"] = customerId
            _this_item["basketId"] = basketId
            _this_item["timeStamp"] = timeStamp
            _this_item["storeId"] = storeId
            #~ bring in the actual product details nest (6 fields)
            _this_item.update(item)
            #~ add the whole lot to our item list
            items.append(_this_item)

    outfile_name = args.input.name.replace(".json", ".csv")

    with open(outfile_name, "w") as csv_outfile:

        #~ create the csv writer object
        csv_writer = csv.writer(csv_outfile)

        #~ write the header / column names
        csv_writer.writerow(_this_item.keys())

        for item in items:
            #~ write row
            csv_writer.writerow(item.values())

    print(f"OK, JSON {args.input.name} converted to CSV {outfile_name}")


def main():

    json_items_to_csv_file(args.input.name)


if __name__ == "__main__":

    main()
