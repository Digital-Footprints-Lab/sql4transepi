"""
This is all the combinations of SQL queries that can be made
from the four major query options:
customer
product
date (or start_date + end_date)
store

Each returns a Template, for sql substitution in main scripts.
"""

from string import Template

#~ QUERIES FUNCTIONS start #########################
#~ FOUR query args #################################
def customer_records_for_product_from_date_from_store(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


def customer_records_for_product_from_date_range_from_store(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


#~ THREE query args ################################
def customer_records_from_store_from_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


def customer_records_from_store_from_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND STORE LIKE '%$store%'
            AND ID = '$customer';""")


def product_records_from_store_from_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")


def product_records_from_store_from_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")


def customer_records_for_product_from_store(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product'
            AND STORE LIKE '%$store%';""")


def customer_records_for_product_from_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND DATE2 = '$date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND DATE2 = '$date'
            AND ITEM_CODE = '$product';""")


def customer_records_for_product_from_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")


#~ TWO query args ##################################
def customer_records_for_product(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer'
            AND ITEM_CODE = '$product';""")


def customer_records_from_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ID = '$customer';""")


def customer_records_from_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ID = '$customer';""")


def product_records_for_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date'
            AND ITEM_CODE = '$product';""")


def product_records_for_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date'
            AND ITEM_CODE = '$product';""")


def store_records_for_customer(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND ID = '$customer';""")


def store_records_for_product(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND ITEM_CODE = '$product';""")


def store_records_for_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND DATE2 = '$date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND DATE2 = '$date';""")


def store_records_for_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%'
            AND DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")


#~ ONE query arg ###################################
def all_records_from_product(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ITEM_CODE = '$product';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ITEM_CODE = '$product';""")


def all_records_from_date(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 = '$date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 = '$date';""")


def all_records_from_date_range(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE DATE2 >= '$start_date'
            AND DATE2 <= '$end_date';""")


def all_records_from_customer(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE ID = '$customer';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE ID = '$customer';""")


def all_records_from_store(join):

    if join:
        return Template("""
            SELECT $record_type FROM $table
            LEFT JOIN $product_table
            ON $card_table.ITEM_CODE = $product_table.productid
            WHERE STORE LIKE '%$store%';""")
    else:
        return Template("""
            SELECT $record_type FROM $table
            WHERE STORE LIKE '%$store%';""")

