#! /usr/bin/env python3

import io
import itertools
import numpy as np
from numpy.random import Generator, PCG64
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import python_snappy
import shutil
import struct
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TFileObjectTransport

import parquet_format.ttypes as fmt


def parse_table(
    table,
    path,
    path_out,
):
    f = open(path, "rb")
    fout = open(path_out, "rb")
    fsize = f.seek(0, io.SEEK_END)
    assert fsize >= 12
    f.seek(0)

    trans = TFileObjectTransport(f)
    protocol = TCompactProtocol(trans)

    magic = f.read(4)
    assert magic == b"PAR1"

    f.seek(fsize - 8)
    footer_len = struct.unpack("<I", f.read(4))[0]
    magic = f.read(4)
    assert footer_len > 0
    assert magic == b"PAR1"
    metadata_pos = fsize - 8 - footer_len
    assert metadata_pos > 0

    f.seek(metadata_pos)
    metadata = fmt.FileMetaData()
    metadata.read(protocol)

    f.seek(0)
    shutil.copyfileobj(f, fout)

    f.seek(4)
    for row_group in metadata.row_groups:
        for column_chunk in row_group.columns:
            # print(repr(column_chunk))
            print(repr(column_chunk.meta_data))
            column_meta_data = column_chunk.meta_data

            # metadata_offset = column_chunk.file_offset
            # f.seek(metadata_offset)
            # column_meta_data = fmt.ColumnMetaData()
            # column_meta_data.read(protocol)
            print(repr(column_meta_data))

            f.seek(column_meta_data.data_page_offset)

            while f.tell() < column_chunk.file_offset:
                page_hdr = fmt.PageHeader()
                page_hdr.read(protocol)
                print(repr(page_hdr))

                if page_hdr.type == fmt.PageType.DATA_PAGE:
                    # pos = f.tell()
                    compressed_data = f.read(page_hdr.compressed_page_size)
                    ...
                elif page_hdr.type == fmt.PageType.DATA_PAGE_V2:
                    f.seek(page_hdr.compressed_page_size, io.SEEK_CUR)
                    ...
                else:
                    pass
                    f.seek(page_hdr.compressed_page_size, io.SEEK_CUR)

    f.close()
    fout.close()

    assert f.stat().st_size == fout.stat().st_size
