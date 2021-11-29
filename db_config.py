"""Some configuration details for Postgres interactions"""

#~ default local database - you will need to supply your username.
config = {
    "database": "TE_DB",
    "user": "at9362",
    "password": "password",
    "host": "127.0.0.1",
    "port": "5432"}

#~ the names of tables, mostly used for sql.substitute commands.
#~ you can rename these here.
boots_transactions = "boots_transactions"
boots_products = "boots_products"
tesco_transactions = "tesco_transactions"
tesco_products = "tesco_products"
dunn_humby = "dunn_humby"
food_products = "food_products"