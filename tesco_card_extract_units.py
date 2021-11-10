import re

#~ make df of products?

"""
Fill column one with the full text field.

Second field = number related to units (eg, 10, 500, 1,)
Third field = units (eg, ml, g, kg, sheets, pack)

1. take last string with number (decimal or not) in
    2. if has letter at end,
        a. if letters are g mg kg - grams
        b. if letters are l ml - litres
    3. if no letters in number && following string
        a. if letters are g mg kg - grams
        b. if letters are l ml - litres
        c. if neither, word becomes unit (eg "sheets")
    4. if no letters or strings following number
        a. leave unit blank

"""