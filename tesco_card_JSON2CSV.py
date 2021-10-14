import json
import csv
import argparse
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
    Tesco card JSONs do not have a unique user identifier:
    the card number is obfuscated, and there are no other
    identifiers to use. This function generates a hex hash,
    using the information block of "Clubcard Accounts" as the hashable.
    It is assumed this block will never change between card JSONs
    from the same customer, but I am not sure if that assumption is safe.

    Args:       A JSON file of Tesco Clubcard user details and transactions

    Returns:    A unique hex identifier.
    """

    with open(json_file) as json_infile:
        data = json.load(json_infile)

    hash_input = data["Customer Profile And Contact Data"]["Clubcard Accounts"]
    cust_hash = hashlib.sha1(str(hash_input).encode())
    cust_hex = cust_hash.hexdigest()

    print(f"\nAssigning hash as customer identifier: {cust_hex}")

    return cust_hex


def json_items_to_csv_file(json_file, customer_id):

    """
    Does the JSON nest-diving to create a single row per item.
    In the JSON, each item does not have useful things like timestamps,
    customerId, etc, so those are extracted from other nests and
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
            #~ add storeId
            item["storeId"] = storeId
            #~ add timeStamp
            item["timeStamp"] = timeStamp
            #~ add our hash-generated customer ID
            item["customerId"] = customer_id
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
