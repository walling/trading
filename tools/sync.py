#!/usr/bin/env python3

import pyarrow.parquet as pq
import fsspec, time, re
from pathlib import Path

# Paths to sync from and to
remote_url = "https://bjarke.me/s/dataset_ygwwjag5qcb8odisldw1kooszvym3qer/trades"
local_url = f"file://{Path(__file__).parent.joinpath('../data/trades').resolve()}"

# Only sync trading data files
FILE_REGEX = re.compile(
    r"^\d{4}(?:-\d{2}(?:-\d{2})?)?\.[\w\-]+\.[\w\-]+\.[\w\-]+\.[\w\-]+.parquet$"
)


def find_parquet_dirs(path, fs):
    """
    Recursively find directories that contain parquet files and a set of the parquet filenames.
    """
    path_cut = len(path) + 1
    for root, dirs, files in fs.walk(path):
        parquet_files = set([file for file in files if FILE_REGEX.fullmatch(file)])
        if len(parquet_files) == 0:
            continue
        yield (root[path_cut:], parquet_files)


def find_parquet_files(path, fs):
    """
    Get the set of parquet filenames in a directory.
    """
    for path, files in find_parquet_dirs(path, fs):
        return files
    return set()


def human_size(size):
    """
    Human readable formatting of file sizes.
    """
    if size < 900:
        return "%db" % size
    elif size < 1024 * 900:
        return "%.1fkb" % (size / 1024)
    else:
        return "%.1fmb" % (size / 1024 / 1024)


# Access file systems
remote = fsspec.open(remote_url)
local = fsspec.open(local_url)

# Loop remote directories containing parquet files
for path, remote_files in find_parquet_dirs(remote.path, remote.fs):
    print(f"CHECKING {path}")

    # Calculate paths and get local files
    remote_path = f"{remote.path}/{path}"
    local_path = f"{local.path}/{path}"
    local_files = find_parquet_files(local_path, local.fs)

    # Find diffs of remote and local file sets
    sync_files = remote_files - local_files
    del_files = local_files - remote_files

    # Also sync files not having the same size
    for file in remote_files:
        if file in sync_files:
            continue

        # Get sizes
        remote_size = remote.fs.info(f"{remote_path}/{file}")["size"]
        local_size = local.fs.info(f"{local_path}/{file}")["size"]

        # If sizes mismatch, add them to files being synced
        if remote_size != local_size:
            rsize = human_size(remote_size)
            lsize = human_size(local_size)
            print(f"=> SIZE MISMATCH: {file} (remote {rsize} vs. local {lsize})")
            sync_files.add(file)

    # Sync files
    for file in sorted(sync_files):
        # Display progress
        remote_size = remote.fs.info(f"{remote_path}/{file}")["size"]
        print(f"=> SYNCING {file} ({human_size(remote_size)})")

        # Ensure local directory and download file from remote
        local.fs.makedirs(local_path, exist_ok=True)
        remote.fs.get_file(f"{remote_path}/{file}", f"{local_path}/{file}")

        # Safety check 1: Ensure that file sizes match
        local_size = local.fs.info(f"{local_path}/{file}")["size"]
        if local_size != remote_size:
            sizes = f"remote {remote_size}b vs. local {local_size}b"
            raise RuntimeError(f"file size mismatch: {local_path}/{file} ({sizes})")

        # Safety check 2: Read test
        pq.read_table(f"{local_path}/{file}")

    # Remove files
    for file in sorted(del_files):
        print(f"=> REMOVING {file}")

        # For safety, we don't remove, but rename the file, so it doesn't end with .parquet
        local.fs.rename(
            f"{local_path}/{file}",
            f"{local_path}/{file}.del_{int(time.time())}",
        )

print("DONE")
