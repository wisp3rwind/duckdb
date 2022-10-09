#! /usr/bin/env python3

import gzip
import io
import itertools
import lz4
import numpy as np
from numpy.random import Generator, PCG64
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import snappy
import shutil
import struct
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TFileObjectTransport
import zstd

import parquet_format.ttypes as fmt


"""Generate data to test parquet data page decompression."""


COMPRESSION_CODECS = [
    "NONE",
    "SNAPPY",
    "GZIP",
    # Brotli is currently not supported by duckdb
    "BROTLI",
    # This generates the new LZ4_RAW parquet compression, which duckdb does not
    # support
    "LZ4",
    "ZSTD",
]


DATA_PAGE_VERSIONS = [
    "1.0",
    "2.0",
]


def build_table():
    # Init rng in a reproducible way
    rng = Generator(PCG64(12345))


    # Plain table.
    N = 30  # column count
    p = .2  # NULL probability

    columns = {}

    # Integer columns, no nesting, no NULL, no repetition
    columns["plain"] = pa.array(np.arange(N))
    columns["plain_random"] = pa.array(rng.choice(N, N))

    # Mixed dtype struct column, NULLs exist at all levels
    x = pa.array(
        rng.choice(["foo", "bar", "baz"], N),
        mask=rng.choice([True, False], N, p=[p, 1 - p]),
    )
    y = pa.array(
        rng.choice(42, N),
        mask=rng.choice([True, False], N, p=[p, 1 - p]),
    )
    z = pa.StructArray.from_arrays(
        (x, y), names=("string", "int"),
        mask=pa.array(rng.choice([True, False], N, p=[p, 1 - p])),
    )
    columns["nested_nulls"] = z

    # Integer list with variable list length and NULLs
    values = list(range(42)) + [None]
    columns["list"] = pa.array(
        [rng.choice(values, count) for count in rng.choice(20, N)],
        mask=pa.array(rng.choice([True, False], N, p=[p, 1 - p])),
    )

    return pa.Table.from_pydict(columns)


def recompress(codec, comp_data):
    comp_size = len(compressed_data)
    if codec == fmt.CompressionCodec.UNCOMPRESSED:
        raise ValueError()
    elif codec == fmt.CompressionCodec.SNAPPY:
        uncomp_data = snappy.uncompress(comp_data)
    elif codec == fmt.CompressionCodec.GZIP:
        uncomp_data = gzip.decompress(comp_data)
    elif codec == fmt.CompressionCodec.LZO:
        # not supported by duckdb
        raise NotImplementedError()
    elif codec == fmt.CompressionCodec.BROTLI:
        # not supported by duckdb
        raise NotImplementedError()
    elif codec == fmt.CompressionCodec.LZ4:
        # Deprecated compression format with a non-standard framing scheme.
        # Not supported by duckdb, and current pyarrow doesn't even write this
        # anymore.
        raise NotImplementedError()
    elif codec == fmt.CompressionCodec.ZSTD:
        uncomp_data = zstd.uncompress(comp_data)
    elif codec == fmt.CompressionCodec.LZ4_RAW:
        # New LZ4 scheme, not supported by duckdb
        raise NotImplementedError()
    else:
        raise NotImplementedError()

    uncomp_size = len(uncomp_data)
    uncomp_data = (uncomp_data[:uncomp_size // 2] + b"A" * (2 * uncomp_size))

    if codec == fmt.CompressionCodec.SNAPPY:
        comp_data = snappy.compress(uncomp_data)
    elif codec == fmt.CompressionCodec.GZIP:
        comp_data = gzip.compress(uncomp_data)
    elif codec == fmt.CompressionCodec.ZSTD:
        comp_data = zstd.compress(uncomp_data, level=6)
    else:
        assert False

    # Be sure that the added constant b"A" compressed well
    # enough to fit the original space in the file. This
    # might leave some garbage after the compressed data,
    # but that shouldn't matter.
    assert len(compressed_data) <= comp_size

    return comp_data

def rewrite_pq_table(path, path_out):
    """Read a parquet file from `path' and iterate over the Thrift structures. Write a
    potentially modified file to `path_out`.
    """
    f = open(path, "rb")
    fout = open(path_out, "wb")
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
                    pos = f.tell()
                    fout.seek(pos)
                    compressed_data = f.read(page_hdr.compressed_page_size)
                    compressed_data = recompress(
                        column_meta_data.codec,
                        compressed_data,
                    )
                    fout.write(compressed_data)
                elif page_hdr.type == fmt.PageType.DATA_PAGE_V2:
                    raise NotImplementedError()
                    f.seek(page_hdr.compressed_page_size, io.SEEK_CUR)
                else:
                    # Not a data page
                    f.seek(page_hdr.compressed_page_size, io.SEEK_CUR)

    f.close()
    fout.close()

    assert path.stat().st_size == path_out.stat().st_size


table = build_table()

root = Path("generated")
root.mkdir(exist_ok=True)

for compression, data_page_version in itertools.product(COMPRESSION_CODECS, DATA_PAGE_VERSIONS):
    pq_args = { 
        "data_page_version": data_page_version,
        "compression": compression,
    }

    basename = "_".join([
        f"data_page={data_page_version[0]}",
        f"{compression}",
    ])

    pq.write_table(
        table,
        (root / basename).with_suffix(".parquet"),
        **pq_args
    )

    if compression == "NONE":
        continue

    try:
        rewrite_pq_table(
            (root / basename).with_suffix(".parquet"),
            (root / f"{basename}-size_mismatch").with_suffix(".parquet"),
        )
    except NotImplementedError:
        pass
