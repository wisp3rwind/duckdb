#! /usr/bin/env sh

if [ ! -e parquet.thrift ]; then
	curl -o parquet.thrift \
		https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift
fi

thrift --gen py parquet.thrift
rm -r parquet_format
mv -T gen-py/parquet parquet_format
rm -r gen-py

./generate-data.py
