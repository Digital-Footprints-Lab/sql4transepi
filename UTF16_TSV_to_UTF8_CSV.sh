#!/bin/bash

iconv -f UTF-16 -t UTF-8 $1 | sed 's/'$'\t''/,/g' > UTF8_$1

