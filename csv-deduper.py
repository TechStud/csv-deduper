#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
#######################################################################################
## CSV Deduper
## - Find and remove duplicate rows in CSV files.
## Author : TechStud
## Version: 0.2.1
## Date   : 2025-04-25
## Github : https://github.com/TechStud/csv-deduper
#######################################################################################
"""
__version__ = "1.2.1"

import pandas as pd
import argparse
import re
import sys
import os
import time
from datetime import datetime
import platform
import subprocess

## User editable Variables
## Set to True if you want to Show or Hide the Progress bar output after processing has concluded.
show_progress = True 
default_chunk_size = 10000 # Safe starting limit


## Variables - Do not edit these
datetimeFormat = '%Y-%m-%d %H:%M:%S.%f'
NOT_PROVIDED = 'NOT_PROVIDED'
file = [NOT_PROVIDED]
columns = [NOT_PROVIDED]
keep = [NOT_PROVIDED]
sortcolumn = [NOT_PROVIDED]
sortorder = [NOT_PROVIDED]
chunksize = [NOT_PROVIDED]
max_chunk_size = 500000    # Safe upper limit

class attr: #Text Attributes
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    BOLD = '\033[1m'
    ITALIC = '\033[3m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def app_logo():
    print(f'''{attr.BOLD}
   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝{attr.END}
{attr.ITALIC}01000011 01010011 01010110 01000100 01000101 01000100 01010101 01010000 01000101 01010010{attr.END}\n''')

def get_file_size(file_path):
    size_bytes = os.path.getsize(file_path)
    if size_bytes < 1024:
        return f"{size_bytes} Bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.2f} MB"

def get_row_count_fast(file_path):
    """Attempts to get the row count quickly using OS-specific commands."""
    os_name = platform.system()
    if os_name == "Linux" or os_name == "Darwin":  # Darwin is macOS
        try:
            result = subprocess.run(['wc', '-l', file_path], capture_output=True, text=True, check=True)
            return int(result.stdout.strip().split()[0]) - 1 
        except subprocess.CalledProcessError:
            print("Warning: 'wc -l' command failed. Falling back to pandas for row count.")
            return None
    elif os_name == "Windows":
        print("Warning: Fast row count not available on Windows. Falling back to pandas for row count.")
        return None
    else:
        print(f"Warning: Operating system '{os_name}' not recognized for fast row count. Falling back to pandas.")
        return None

def deduplicate_csv_enhanced(input_file, columns, output_file, keep, sort_column, sort_order, chunk_size):
    start_elapsed_time = time.time()
    start_time = datetime.now()
    total_rows = None
    processed_rows = 0
    unique_chunks = []
    fast_row_count = get_row_count_fast(input_file)
    if fast_row_count is not None:
        total_rows = fast_row_count
    else:
        approx_total_rows = 0
        try:
            for _ in pd.read_csv(input_file, chunksize=chunk_size, iterator=True):
                approx_total_rows += chunk_size
            total_rows = approx_total_rows 
        except Exception as e:
            print(f"Warning: Could not estimate total rows: {e}")
    reader = pd.read_csv(input_file, chunksize=chunk_size, iterator=True)
    for i, chunk in enumerate(reader):
        if columns:
            chunk_dropped = chunk.drop_duplicates(subset=columns, keep=keep)
        else:
            chunk_dropped = chunk.drop_duplicates(keep=keep)
        unique_chunks.append(chunk_dropped)
        processed_rows += len(chunk)
        if total_rows:
            progress = min(100, (processed_rows / total_rows) * 100)
            elapsed_time = (time.time() - start_elapsed_time)
            print(f" {attr.BOLD}Processing...{attr.END} [{'#'*int(progress//2)}{'.'*(50-int(progress//2))}] {attr.BOLD}{progress:.0f}%{attr.END} | Time: {elapsed_time:.2f} sec | [{processed_rows:,}/{total_rows:,}]", end='\r', flush=True)
        elif approx_total_rows > 0:
            progress = min(100, (processed_rows / approx_total_rows) * 100)
            elapsed_time = (time.time() - start_elapsed_time)
            print(f"{attr.BOLD}Processing...{attr.END} [{'#'*int(progress//2)}{'.'*(50-int(progress//2))}] {attr.BOLD}{progress:.0f}%{attr.END} | Time: {elapsed_time:.2f} sec | Processed: {processed_rows:,} (Total estimate: {approx_total_rows:,})", end='\r')
        else:
            elapsed_time = (time.time() - start_elapsed_time)
            print(f"{attr.BOLD}Processing chunk {i+1}{attr.END}... Time: {elapsed_time:.2f} sec | Processed: {processed_rows:,}", end='\r')
    print("\n") if show_progress else print("\x1b[2K", flush=True)
    df_unique = pd.concat(unique_chunks, ignore_index=True)
    if columns:
        df_unique.drop_duplicates(subset=columns, keep=keep, inplace=True)
    else:
        df_unique.drop_duplicates(keep=keep, inplace=True)
    if sort_column:
        df_unique.sort_values(by=sort_column, ascending=(sort_order == 'asc'), inplace=True)
    df_unique.to_csv(output_file, index=False)
    end_time = datetime.now()
    processing_diff = end_time - start_time
    processing_diff_sec = processing_diff.total_seconds()
    if processing_diff_sec < 1: 
        processing_time = f"{(processing_diff_sec * 1000):.2f} ms"
    else: 
        processing_time = f"{processing_diff_sec:.2f} sec"
    filesize_diff = subtract_file_sizes(input_filesize_str, output_filesize_str)
    filesize_percent = (input_filesize_bytes - output_filesize_bytes) / input_filesize_bytes
    output_row_count = len(df_unique)
    final_total_rows = total_rows if total_rows is not None else processed_rows 
    dropped_rows = final_total_rows - output_row_count if final_total_rows is not None else "N/A"
    dropped_percent = (total_rows - output_row_count) / total_rows
    width = 12
    print(f"{attr.BOLD} [ Processing Complete ]{attr.END}", flush=True)
    print(f"\u200B {attr.BOLD}{'Input File'.rjust(width)} :{attr.END} {attr.ITALIC}{input_file_path}/{attr.END}{attr.BOLD}{attr.BLUE}{input_file_name}{attr.END} ")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳ {total_rows:,} rows = {input_filesize_str}{attr.END} ")
    if columns_to_dedupe == None:
        print(f"\u200B {attr.ITALIC}{'criteria'.rjust(width)}{attr.END} {attr.BOLD}: {attr.BLUE}↳{attr.END} Match duplicate rows based on {attr.BOLD}{attr.BLUE}all columns{attr.END}.")
    else:
        print(f"\u200B {attr.ITALIC}{'criteria'.rjust(width)}{attr.END} {attr.BOLD}: {attr.BLUE}↳{attr.END} Match duplicate rows based on these columns: {attr.BOLD}{attr.BLUE}'{columns_to_dedupe_str}'{attr.END}")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END} Keep the {attr.BOLD}{attr.BLUE}{keep_option}{attr.END} occurance of any duplicates and drop the remaining")
    if sort_order is None and sort_column is None:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END} Final sorting {attr.BOLD}{attr.BLUE}not{attr.END} applied")
    else:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END} Final sorting applied to all rows based on {attr.BOLD}{attr.BLUE}'{sort_column}'{attr.END} in {attr.BOLD}{attr.BLUE}{sort_order}{attr.END} order")
    print("")
    print(f"\u200B {attr.BOLD}{'Output File'.rjust(width)} :{attr.END} {attr.ITALIC}{input_file_path}/{attr.END}{attr.BOLD}{attr.BLUE}{output_file_name}{attr.END} ")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳ {output_row_count:,} rows = {output_filesize_str}{attr.END} ")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END}{attr.ITALIC} {attr.BOLD}{dropped_rows:,} rows{attr.END}{attr.ITALIC} were Deduped ({dropped_percent:.2%}{attr.END})")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END}{attr.ITALIC} Resulting in a {attr.BOLD}{filesize_diff}{attr.END}{attr.ITALIC} file Reduction ({filesize_percent:.2%}{attr.END})")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END}{attr.ITALIC} Processing completed in {attr.BOLD}{attr.BLUE}{processing_time}{attr.END}")
    if chunk_size != default_chunk_size:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}  ↳{attr.END}{attr.ITALIC} Using default ChunkSize: {attr.BOLD}{attr.BLUE}{chunk_size:,} lines{attr.END}")
    else:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}  ↳{attr.END}{attr.ITALIC} Using ChunkSize: {attr.BOLD}{attr.BLUE}{chunk_size:,} lines{attr.END}")
    print("")

def parse_file_size(file_size_str):
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4
    }
    match = re.match(r'(\d+(\.\d+)?)\s*([KMGT]B|B)', file_size_str)
    if not match:
        raise ValueError(f"Invalid file size format: {file_size_str}")
    value, unit = float(match.group(1)), match.group(3)
    return value * units[unit]

def format_file_size(size_in_bytes):
    units = [
        (1024**4, 'TB'),
        (1024**3, 'GB'),
        (1024**2, 'MB'),
        (1024, 'KB'),
        (1, 'B')
    ]
    for factor, suffix in units:
        if size_in_bytes >= factor:
            return f"{size_in_bytes / factor:.2f} {suffix}"
    return f"{size_in_bytes} B"

def subtract_file_sizes(input_filesize_str, output_filesize_str):
    input_size_bytes = parse_file_size(input_filesize_str)
    output_size_bytes = parse_file_size(output_filesize_str)
    result_size_bytes = input_size_bytes - output_size_bytes
    result_size_str = format_file_size(result_size_bytes)
    return result_size_str

if __name__ == "__main__":
    app_logo()
    parser = argparse.ArgumentParser(description="Deduplicate a CSV file based on specified columns with options to keep first/last duplicate and sort.")
    parser.add_argument("file", help="Path to the unfiltered CSV data file. Enclose in double quotes if path and/or filename have spaces (\"/path to/unfiltered data.csv\").")
    parser.add_argument("-c", "--columns", nargs=1, default=[NOT_PROVIDED], help="Optional. Comma-separated list of column headers to check for duplicates. Enclose in single/double quotes. (ie: \"header 1,header 3\")")
    parser.add_argument("-k", "--keep", nargs=1, choices=['first', 'last'], default=[NOT_PROVIDED], help="Optional. Specify 'first' to keep the first occurrence (default) or 'last' to keep the last.")
    parser.add_argument("-sc", "--sortcolumn", nargs=1, default=[NOT_PROVIDED], help="Optional. Column header to sort by. Only one column. Enclose in single/double quotes. (ie: \"header 3\")")
    parser.add_argument("-so", "--sortorder", nargs=1, choices=['asc', 'desc'], default=[NOT_PROVIDED], help="Optional. Sort order ('asc' for ascending, 'desc' for descending). Requires '--sortcolumn'")
    parser.add_argument("-ch", "--chunksize", nargs=1, type=int, default=[NOT_PROVIDED], help=f"Optional. Chunk-size for reading large CSV (default: {default_chunk_size})")
    args = parser.parse_args()
    args_list = args
    input_file = args.file.strip('"')                       # Remove potential double quotes
    input_file_name = os.path.basename(input_file)          # Just the filename (no path)
    input_file_fpath = os.path.realpath(input_file)         # Full path to filename eg: /home/usr/.../filename.ext
    input_file_path = os.path.dirname(input_file_fpath)     # Just the full path (no filename)
    input_filesize_str = get_file_size(input_file)          # Filesize (as reported by the system) with B/KB/MB.. incl. (string)
    input_filesize_bytes = os.path.getsize(input_file)      # Filesize in bytes (numeric)
    output_file = f"{os.path.splitext(input_file)[0]}_csv_deduped.csv"
    with open(output_file, 'a') as f:
        pass  # This will create the file if it doesn't exist
    output_file_name = os.path.basename(output_file)
    output_file_fpath = os.path.realpath(output_file)
    output_filesize_str = get_file_size(output_file)
    output_filesize_bytes = os.path.getsize(output_file)
    columns_to_dedupe = [col.strip('"').strip() for col in args.columns[0].split(',')] if args.columns != [NOT_PROVIDED] else None
    columns_to_dedupe_str = '\' & \''.join(columns_to_dedupe) if columns_to_dedupe else None
    keep_option = args.keep[0].strip('"').strip() if args.keep != [NOT_PROVIDED] else 'first'
    sort_column = args.sortcolumn[0].strip('"').strip() if args.sortcolumn != [NOT_PROVIDED] else None
    if args.sortorder == [NOT_PROVIDED] and args.sortcolumn == [NOT_PROVIDED]: 
        sort_order = None
    elif args.sortorder == [NOT_PROVIDED] and args.sortcolumn != [NOT_PROVIDED]:
        sort_order = 'asc'
    else:
        sort_order = args.sortorder[0].strip('"').strip()
    if sort_order is not None and sort_column is None:
        print(f"{attr.BOLD}{attr.RED}Warning:{attr.END} {attr.BOLD}--sort{attr.END} argument was provided {attr.ITALIC}without{attr.END} {attr.BOLD}--sortcolumn{attr.END}. Sorting will be skipped.\n")
    chunk_size = args.chunksize[0] if args.chunksize != [NOT_PROVIDED] else default_chunk_size
    if chunk_size < 1 or chunk_size > max_chunk_size:
        print(f"{attr.BOLD}{attr.RED}Error:{attr.END} Chunk size must be between {attr.BOLD}1 and {max_chunk_size}{attr.END}.")
        exit(1)
    try:
        deduplicate_csv_enhanced(input_file, columns_to_dedupe, output_file, keep_option, sort_column, sort_order, chunk_size)
    except FileNotFoundError:
        print(f"{attr.BOLD}{attr.RED}ERROR:{attr.END} Input file {attr.BOLD}'{input_file}'{attr.END} not found.")
        exit(1)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(f"{attr.BOLD}{attr.RED}An error occurred:{attr.END} {attr.BOLD}{attr.ITALIC}{e}{attr.END}")
        print("")
        print(exc_tb.tb_lineno)
        exit(1)
