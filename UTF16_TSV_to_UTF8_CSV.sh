#!/bin/bash

# Converts UTF16 Tab Separated Values to
# UTF8 Comma Separated Values
# Outputs a file prepended with "UTF8_"

if [[ $# == 0 ]] ; then
    echo "Please provide a file to convert."
    exit 0
fi

iconv -f UTF-16 -t UTF-8 $1 | sed 's/'$'\t''/,/g' > UTF8_$1
echo "OK, converted $1 from UTF16 tab separated to UTF8 comma separated."
echo "Output file name: UTF8_$1"


