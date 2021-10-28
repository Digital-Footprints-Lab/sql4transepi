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


def generate_hash_id(json_file):

    """
    Tesco card JSONs do not have a unique and persistent user identifier:
    the card number is obfuscated, and there are no other
    identifiers to use. This function generates a hex hash,
    using the transaction storeId and timestamp for the first even transaction
    as the hashable. This should be unique and immutable, unless Tesco
    change the format of their JSONs.

    Args:       A JSON file of Tesco Clubcard user details and transactions

    Returns:    A unique 16 char hex identifier.
    """

    with open(json_file) as json_infile:
        data = json.load(json_infile)

    #~ get the storeid and timestamp of their first even transaction
    store_id = data["Purchase"][0][0]["storeId"]
    timestamp = data["Purchase"][0][0]["timeStamp"]
    hash_input = store_id + timestamp
    #~ hash it
    cust_hash = hashlib.sha1(str(hash_input).encode())
    cust_hex = cust_hash.hexdigest()[0:16]

    print(f"\nAssigning hash as customer identifier: {cust_hex}")

    return cust_hex


def json_items_to_csv_file(json_file, customer_id):

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

    #~ The "Purchase" nest is a >>list<< of transactions
    for transaction in data["Purchase"][0]:
        storeId = transaction["storeId"]
        timeStamp = transaction["timeStamp"]
        #~ The "product" nest is a >>dict<< of items in the transaction
        for item in transaction["product"]:
            item = OrderedDict(item)
            #~ add storeId + prepend
            item["storeId"] = storeId
            item.move_to_end("storeId", last=False)
            #~ add timestamp + prepend
            item["timeStamp"] = timeStamp
            item.move_to_end("timeStamp", last=False)
            #~ add our hash-generated customer ID + prepend
            item["customerId"] = customer_id
            item.move_to_end("customerId", last=False)
            #~ add the whole lot to our item list
            items.append(item)

    outfile_name = args.input.name.replace(".json", ".csv")

    with open(outfile_name, "w") as csv_outfile:

        #~ create the csv writer object
        csv_writer = csv.writer(csv_outfile)

        #~ write the header / column names
        csv_writer.writerow(item.keys())

        for item in items:
            #~ write row
            csv_writer.writerow(item.values())

    print(f"OK, JSON {args.input.name} converted to CSV {outfile_name}")


def main():

    customer_id = generate_hash_id(args.input.name)

    json_items_to_csv_file(args.input.name, customer_id)


if __name__ == "__main__":

    main()
