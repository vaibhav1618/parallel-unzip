#!/usr/bin/python3

from operator import contains
import sys
import tarfile
import os
import multiprocessing as mp
import zstandard as zstd
import tempfile


def unzip_star(fullpath):
    """worker unzips one tar file"""
    print("extracting... {} {}".format(fullpath, os.path.splitext(fullpath)[0]))
    tar = tarfile.open(fullpath)
    tar.extractall(os.path.splitext(fullpath)[0])
    tar.close()


def unzip_zstd(fullpath):
    """worker unzips one tar.zst file"""
    print("extracting... {} {}".format(fullpath, os.path.splitext(fullpath)[0]))
    dctx = zstd.ZstdDecompressor()

    with tempfile.TemporaryFile(prefix=os.path.splitext(fullpath)[0], dir=".") as ofh:
        with open(fullpath, "rb") as ifh:
            read, written = dctx.copy_stream(ifh, ofh)
            # print ("size of {}: {} {}".format(os.path.splitext(fullpath)[0], read, written))
        ofh.seek(0)
        with tarfile.open(fileobj=ofh) as z:
            z.extractall(os.path.dirname(fullpath))
    print("extracting done... {}".format(fullpath))


def fanout_unziptar(path):
    """create pool to extract all"""
    tar_files = []
    zstd_files = []
    for root, dirs, files in os.walk(path):
        for fl in files:
            if fl.endswith(".tar"):
                tar_files.append(os.path.join(root, fl))
            elif fl.endswith(".tar.zst"):
                zstd_files.append(os.path.join(root, fl))
        break
    # print("dir: {} tar_files: {} zstd_files: {}".format(path, len(tar_files), len(zstd_files)))
    if (len(tar_files) == 0 and len(zstd_files) == 0):
        return
    if (len(tar_files) != 0):
        pool = mp.Pool(min(mp.cpu_count(), len(tar_files)))  # number of workers
        pool.map(unzip_star, tar_files, chunksize=1)
        pool.close()
    if (len(zstd_files) != 0):
        pool = mp.Pool(min(mp.cpu_count(), len(zstd_files)))  # number of workers
        pool.map(unzip_zstd, zstd_files, chunksize=1)
        pool.close()
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            fanout_unziptar(os.path.join(root, dir))
        break


if __name__ == "__main__":
    path = sys.argv[1]
    fanout_unziptar(path)
    print("Extraction has completed")
