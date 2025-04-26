#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__version__ = "1.2.2"
"""
####################################################################################################
## CSV Deduper
## Author : TechStud
## Date   : 2025-04-25
## Version: 0.2.2
## License: MIT License
## Github : https://github.com/TechStud/csv-deduper
## Social : https://bsky.app/profile/techstud.com
####################################################################################################
Description: 
  Quickly and efficiently remove duplicate rows within a CSV data file, based on specified columns 
  with options to keep first/last duplicate and sort from a given CSV file, generating a new CSV 
  output file containing only the unique data you need.

Usage: 
  csv-deduper.py [-h] [-c COLUMNS] [-k {first,last}] [-sc SORTCOLUMN] [-so {asc,desc}] [-ch CHUNKSIZE] [-v] file
  -h, --help
            show this help message and exit
  -c COLUMNS, --columns COLUMNS
            Comma-separated list of column headers to check for duplicates. Enclose in single/double 
            quotes. (ie: "header 1,header 3")
  -k {first,last}, --keep {first,last}
            Specify 'first' to keep the first occurrence (default) or 'last' to keep the last.
  -sc SORTCOLUMN, --sortcolumn SORTCOLUMN
            Column header to sort by. Only one column. Enclose in single/double quotes. 
            (ie: "header 3")
  -so {asc,desc}, --sortorder {asc,desc}
            Sort order ('asc' for ascending, 'desc' for descending). Requires '--sortcolumn'
  -ch CHUNKSIZE, --chunksize CHUNKSIZE
            Chunk-size for reading large CSV (default: 10000)
  -v, --version
           show program's version number and exit

Dependencies:
  - pandas - Required for data manipulation
           - 'pip install pandas' to install
####################################################################################################
"""

import pandas as pd             # Pandas is a powerful library for data manipulation and analysis
import argparse                 # used for parsing command-line arguments
import shutil                   # provides high-level file operations (copying, moving, archiving, etc.)
import re                       # provides support for regular expressions
import sys                      # provides access to system-specific parameters and functions
import os                       # provides a way of using operating system-dependent functionality 
import time                     # provides time-related functions
import signal                   # provides mechanisms to handle asynchronous events (signals)
from datetime import datetime   # provides classes for working with dates and times
import platform                 # provides access to underlying platform's identifying data
import subprocess               # provides the ability to spawn new processes, connect to their input/output/error pipes, and obtain their return codes

## User editable Variables
show_progress = True            # Set to True if you want to Show or False to Hide the Progress bar output after processing has concluded.
default_chunk_size = 10000      # Safe starting limit for reading the CSV file in chunks.

## Variables - Do not edit these (unless you know what you're doing!)
datetimeFormat = '%Y-%m-%d %H:%M:%S.%f' # Format string for datetime objects (currently not directly used)
NOT_PROVIDED = 'NOT_PROVIDED'           # Sentinel value to indicate an argument was not provided
file = [NOT_PROVIDED]                   # List to hold the input file path (initially not provided)
columns = [NOT_PROVIDED]                # List to hold the column names for deduplication (initially not provided)
keep = [NOT_PROVIDED]                   # List to hold the 'keep' option ('first' or 'last') (initially not provided)
sortcolumn = [NOT_PROVIDED]             # List to hold the column name for sorting (initially not provided)
sortorder = [NOT_PROVIDED]              # List to hold the sorting order ('asc' or 'desc') (initially not provided)
chunksize = [NOT_PROVIDED]              # List to hold the chunk size for reading the CSV (initially not provided)
max_chunk_size = 500000                 # Safe upper limit for the chunk size to prevent excessive memory usage
elapsed_time = 0                        # Variable to track the elapsed processing time
bar_length = 40                         # Initial length of the progress bar
current_iteration = 0                   # Counter for the current processing iteration (used for progress bar)
total_iterations = 100                  # Total number of iterations (initially a placeholder, updated later)

class attr: #Text Attributes - ANSI escape codes for colored terminal output
    CYAN = '\033[96m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    ITALIC = '\033[3m'
    END = '\033[0m'

def app_logo():
    """Clears the terminal and prints the application's ASCII art logo."""
    clear_terminal()
    print(f'''{attr.BOLD}
   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝ v{__version__}{attr.END}
{attr.ITALIC}01000011 01010011 01010110 01000100 01000101 01000100 01010101 01010000 01000101 01010010{attr.END}\n''')

def get_file_size(file_path):
    """
    Gets the human-readable size of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The file size in bytes, KB, or MB (whichever is most appropriate).
    """
    size_bytes = os.path.getsize(file_path)
    if size_bytes < 1024:
        return f"{size_bytes} Bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.2f} MB"

def get_row_count_fast(file_path):
    """
    Attempts to get the row count of a CSV file quickly using OS-specific commands.
    This method avoids loading the entire file into memory.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        int: The number of rows (excluding the header) if successful, otherwise None.
    """
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

def clear_terminal():
    """Clears the terminal screen based on the operating system."""
    system = platform.system()
    if system == "Windows":
        os.system('cls')
    elif system == "Linux" or system == "Darwin":  # Darwin is macOS
        os.system('clear')
    else:
        print(f"Operating system '{system}' not recognized for terminal clearing.")
        print("Continuing without clearing the screen.")

def update_bar_length():
    """Updates the length of the progress bar based on the current terminal width."""
    global bar_length
    columns = shutil.get_terminal_size().columns
    bar_length = max(10, columns - 40)              # Ensure the bar length is at least 10 and fits within the terminal

def progress_bar(iteration, total, elapsed_time):
    """
    Displays a text-based progress bar in the terminal.

    Args:
        iteration (int): The current iteration or number of processed rows.
        total (int): The total number of rows to process.
        elapsed_time (float): The time elapsed since the start of processing.
    """
    elapsed_time_str = elapsed_time
    if elapsed_time < 1: 
        elapsed_time_str = f"{(elapsed_time * 1000):.2f} ms"
    else: 
        elapsed_time_str = f"{elapsed_time:.2f} sec"
    percent = float(iteration) / total
    arrow = '=' * int(round(percent * bar_length) - 1) + '>'
    spaces = ' ' * (bar_length - len(arrow))
    sys.stdout.write(f"\r {attr.BOLD}Deduping...{attr.END} [{attr.BLUE}{arrow}{attr.END}{spaces}] {attr.BOLD}{percent:.0%} | Time: {elapsed_time_str}{attr.END}")
    sys.stdout.flush()

def handle_resize(signal, frame):
    """
    Handles the terminal window resize signal (SIGWINCH).
    Updates the progress bar length and redraws the bar.

    Args:
        signal (int): The signal number.
        frame (frame object): The current stack frame.
    """
    update_bar_length()
    """ Clear the current line and redraw the progress bar to ensure it's aligned correctly"""
    sys.stdout.write("\r" + " " * (bar_length + 40) + "\r")
    progress_bar(current_iteration, total_iterations, elapsed_time)

""" Register the signal handler for SIGWINCH to handle terminal resizing and redraw the progress bar"""
signal.signal(signal.SIGWINCH, handle_resize)

""" Initial update of bar length when the script starts """
update_bar_length()

def deduplicate_csv_enhanced(input_file, columns, output_file, keep, sort_column, sort_order, chunk_size):
    """
    Reads a CSV file in chunks, removes duplicate rows based on specified columns,
    keeps either the first or last occurrence, sorts the data if requested, and
    writes the unique data to a new CSV file.

    Args:
        input_file (str): Path to the input CSV file.
        columns (list or None): List of column headers to check for duplicates. If None, checks all columns.
        output_file (str): Path to the output CSV file where unique rows will be written.
        keep (str): 'first' to keep the first duplicate, 'last' to keep the last.
        sort_column (str or None): Column header to sort by. If None, no sorting is performed.
        sort_order (str or None): 'asc' for ascending, 'desc' for descending. Requires sort_column.
        chunk_size (int): Number of rows to read into memory at a time.
    """
    start_elapsed_time = time.time()    # Record the starting time for overall processing
    start_time = datetime.now()         # Record the starting datetime for more detailed timing
    total_rows = None                   # Initialize the total number of rows
    processed_rows = 0                  # Initialize the count of processed rows
    unique_chunks = []                  # List to store unique chunks of data
    fast_row_count = get_row_count_fast(input_file) # Attempt to get a fast row count
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

    reader = pd.read_csv(input_file, chunksize=chunk_size, iterator=True) # Read the CSV file in chunks
    for i, chunk in enumerate(reader):
        global current_iteration, total_iterations, elapsed_time # Access global variables for progress tracking
        current_iteration = processed_rows # Update the current iteration for the progress bar
        total_iterations = total_rows if total_rows else (approx_total_rows // chunk_size + 1 if approx_total_rows > 0 else 'unknown') # Update total iterations for progress bar
        
        if columns:
            chunk_dropped = chunk.drop_duplicates(subset=columns, keep=keep) # Remove duplicates based on specified columns, keeping 'first' or 'last'
        else:
            chunk_dropped = chunk.drop_duplicates(keep=keep)                # Remove duplicates based on all columns, keeping 'first' or 'last'
        unique_chunks.append(chunk_dropped)                                 # Store the chunk of unique rows
        processed_rows += len(chunk)                                        # Increment the count of processed rows

        if total_rows:
            progress = min(100, (processed_rows / total_rows) * 100) 
            elapsed_time = (time.time() - start_elapsed_time)               # Keep track of the elapsed time
            progress_bar(processed_rows,total_rows,elapsed_time)            # Update the progress bar
        elif approx_total_rows > 0:
            progress = min(100, (processed_rows / approx_total_rows) * 100)
            elapsed_time = (time.time() - start_elapsed_time)
            progress_bar(processed_rows,total_rows,elapsed_time)            # Update the progress bar
        else:
            elapsed_time = (time.time() - start_elapsed_time)
            print(f"{attr.BOLD}Processing chunk {i+1}{attr.END}... Time: {elapsed_time:.2f} sec | Processed: {processed_rows:,}", end='\r')

    df_unique = pd.concat(unique_chunks, ignore_index=True)                 # Concatenate all unique chunks into a single DataFrame
    if columns:
        df_unique.drop_duplicates(subset=columns, keep=keep, inplace=True)  # Final deduplication on the combined data (in case duplicates spanned chunks)
    else:
        df_unique.drop_duplicates(keep=keep, inplace=True)                  # Final deduplication on the combined data (in case duplicates spanned chunks)
    if sort_column:
        df_unique.sort_values(by=sort_column, ascending=(sort_order == 'asc'), inplace=True) # Sort the unique DataFrame if a sort column is specified

    df_unique.to_csv(output_file, index=False)                              # Write the unique DataFrame to a new CSV file without the index

    end_time = datetime.now()                                               # Record the ending datetime
    processing_diff = end_time - start_time                                 # Calculate the total processing time
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
    dropped_percent = (total_rows - output_row_count) / total_rows if total_rows else 0 # Handle case where total_rows is None
    width = 12
    
    if show_progress:
        # If user sets show_progress to True, the Progress Bar will remain then print a confirmation below it
        print(f"\n{attr.BOLD}{attr.BLUE} ↳ Deduping Process Completed {attr.END}\n", flush=True)
    else:
        # If user sets show_progress to False, the Progress Bar line be cleared and a confirmation message will be printed on the same line
        print("\x1b[2K", end='\r', flush=True) 
        print(f"{attr.BOLD}{attr.BLUE} Deduping Process Completed {attr.END}\n", flush=True)

    
    print(f"\u200B {attr.BOLD}{'Input File'.rjust(width)} :{attr.END} {attr.ITALIC}{input_file_path}/{attr.END}{attr.BOLD}{attr.BLUE}{input_file_name}{attr.END} ")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳ {total_rows:,} rows = {input_filesize_str}{attr.END} ")
    if columns_to_dedupe == None:
        print(f"\u200B {attr.ITALIC}{attr.CYAN}{'criteria'.rjust(width)}{attr.END} {attr.BOLD}: {attr.BLUE}↳{attr.END} Matching duplicate rows based on {attr.BOLD}{attr.BLUE}all columns{attr.END}.")
    else:
        print(f"\u200B {attr.ITALIC}{attr.CYAN}{'criteria'.rjust(width)}{attr.END} {attr.BOLD}: {attr.BLUE}↳{attr.END} Matching duplicate rows based on these columns: {attr.BOLD}{attr.BLUE}'{columns_to_dedupe_str}'{attr.END}")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END} Keeping the {attr.BOLD}{attr.BLUE}{keep_option}{attr.END} occurance of any duplicates and dropping the remaining")
    if sort_order is None and sort_column is None:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END} Final sorting will {attr.BOLD}{attr.BLUE}not{attr.END} be applied")
    else:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END} Final sorting will be applied to all rows based on {attr.BOLD}{attr.BLUE}'{sort_column}'{attr.END} in {attr.BOLD}{attr.BLUE}{sort_order}{attr.END} order")
    print("")
    print(f"\u200B {attr.BOLD}{'Output File'.rjust(width)} :{attr.END} {attr.ITALIC}{input_file_path}/{attr.END}{attr.BOLD}{attr.BLUE}{output_file_name}{attr.END} ")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳ {output_row_count:,} rows = {output_filesize_str}{attr.END} ")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END}{attr.ITALIC} {attr.BOLD}{dropped_rows:,} rows{attr.END}{attr.ITALIC} were removed ({dropped_percent:.2%}{attr.END})")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END}{attr.ITALIC} Resulting in a {attr.BOLD}{filesize_diff}{attr.END}{attr.ITALIC} file reduction ({filesize_percent:.2%}{attr.END})")
    print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}↳{attr.END}{attr.ITALIC} Total Processing completed in {attr.BOLD}{attr.BLUE}{processing_time}{attr.END}")
    if chunk_size != default_chunk_size:
        print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}  ↳{attr.END}{attr.ITALIC} Using ChunkSize: {attr.BOLD}{attr.BLUE}{chunk_size:,} lines{attr.END}")
    # else:
        # print(f"\u200B {attr.BOLD}{' '.rjust(width)} :{attr.END} {attr.BOLD}{attr.BLUE}  ↳{attr.END}{attr.ITALIC} Using default ChunkSize: {attr.BOLD}{attr.BLUE}{chunk_size:,} lines{attr.END}")
    print("") # Have a clean/empty line before the commline prompt

def parse_file_size(file_size_str):
    """
    Parses a human-readable file size string (e.g., "1.23 MB") into bytes.

    Args:
        file_size_str (str): The file size string.

    Returns:
        int: The file size in bytes.

    Raises:
        ValueError: If the file size format is invalid.
    """
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4
    }
    match = re.match(r'(\d+(\.\d+)?)\s*([KMGT]B|B)', file_size_str)     # Case-insensitive matching for units
    if not match:
        raise ValueError(f"Invalid file size format: {file_size_str}")
    value, unit = float(match.group(1)), match.group(3).upper()         # Convert unit to uppercase for consistency
    return value * units[unit]

def format_file_size(size_in_bytes):
    """
    Formats a file size in bytes into a human-readable string (e.g., "1.23 MB").

    Args:
        size_in_bytes (int): The file size in bytes.

    Returns:
        str: The human-readable file size string.
    """
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
    """
    Subtracts two human-readable file size strings and returns the difference
    in a human-readable format.

    Args:
        input_filesize_str (str): The file size of the input file.
        output_filesize_str (str): The file size of the output file.

    Returns:
        str: The difference in file sizes in a human-readable format.
    """
    input_size_bytes = parse_file_size(input_filesize_str)
    output_size_bytes = parse_file_size(output_filesize_str)
    result_size_bytes = input_size_bytes - output_size_bytes
    result_size_str = format_file_size(result_size_bytes)
    return result_size_str

if __name__ == "__main__":
    app_logo() # Display the application logo 
    
    # Initialize the argument parser with a description of the script
    parser = argparse.ArgumentParser(description="Efficiently remove duplicate CSV data based on specified columns with options to keep first/last matched duplicate and sort output.")
    # Define the positional argument for the input file
    parser.add_argument("file", help="Path to the unfiltered CSV data file. Enclose in double quotes if path and/or filename have spaces (\"/path to/unfiltered data.csv\").")
    # Define the optional argument for specifying columns to check for duplicates
    parser.add_argument("-c", "--columns", nargs=1, default=[NOT_PROVIDED], help="Optional. Comma-separated list of column headers to check for duplicates. Enclose in single/double quotes. (ie: \"header 1,header 3\")")
    # Define the optional argument for specifying which duplicate to keep ('first' or 'last')
    parser.add_argument("-k", "--keep", nargs=1, choices=['first', 'last'], default=[NOT_PROVIDED], help="Optional. Specify 'first' to keep the first occurrence (default) or 'last' to keep the last.")
    # Define the optional argument for specifying the column to sort by
    parser.add_argument("-sc", "--sortcolumn", nargs=1, default=[NOT_PROVIDED], help="Optional. Column header to sort by. Only one column. Enclose in single/double quotes. (ie: \"header 3\")")
    # Define the optional argument for specifying the sort order ('asc' or 'desc')
    parser.add_argument("-so", "--sortorder", nargs=1, choices=['asc', 'desc'], default=[NOT_PROVIDED], help="Optional. Sort order ('asc' for ascending, 'desc' for descending). Requires '--sortcolumn'")
    # Define the optional argument for setting the chunk size for reading large files
    parser.add_argument("-ch", "--chunksize", nargs=1, type=int, default=[NOT_PROVIDED], help=f"Optional. Chunk-size for reading large CSV (default: {default_chunk_size})")
    # Define the version argument to display the script's version
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s v{__version__}")
    
    # Parse the command-line arguments
    args = parser.parse_args() 
    
    # Extract and process the input file path, removing potential surrounding quotes
    input_file = args.file.strip('"')                       # Remove potential double quotes
    input_file_name = os.path.basename(input_file)          # Just the filename (no path)
    input_file_fpath = os.path.realpath(input_file)         # Full path to filename eg: /home/usr/.../filename.ext
    input_file_path = os.path.dirname(input_file_fpath)     # Just the full path (no filename)
    input_filesize_str = get_file_size(input_file)          # Filesize (as reported by the system) with B/KB/MB.. incl. (string)
    input_filesize_bytes = os.path.getsize(input_file)      # Filesize in bytes (numeric)
    
    # Construct the output file name based on the input file name
    output_file = f"{os.path.splitext(input_file)[0]}_csv_deduped.csv"
    # Create an empty output file if it doesn't exist (to ensure get_file_size works correctly initially)
    with open(output_file, 'a') as f:
        pass  # This will create the file if it doesn't exist
    output_file_name = os.path.basename(output_file)        # Get the filename of the output file
    output_file_fpath = os.path.realpath(output_file)       # Get the absolute path to the output file
    output_filesize_str = get_file_size(output_file)        # Get the human-readable size of the output file
    output_filesize_bytes = os.path.getsize(output_file)    # Get the size of the output file in bytes
    
    # Process the --columns argument to get a list of columns to dedupe on
    columns_to_dedupe = [col.strip('"').strip() for col in args.columns[0].split(',')] if args.columns != [NOT_PROVIDED] else None
    columns_to_dedupe_str = '\' & \''.join(columns_to_dedupe) if columns_to_dedupe else None    # Create a user-friendly string of the columns
    
    # Process the --keep argument to determine whether to keep the first or last duplicate
    keep_option = args.keep[0].strip('"').strip() if args.keep != [NOT_PROVIDED] else 'first'   # Default to 'first' if not provided

    # Process the --sortcolumn argument to get the column to sort by
    sort_column = args.sortcolumn[0].strip('"').strip() if args.sortcolumn != [NOT_PROVIDED] else None
    
    # Process the --sortorder argument, defaulting to 'asc' if --sortcolumn is provided but --sortorder is not
    if args.sortorder == [NOT_PROVIDED] and args.sortcolumn == [NOT_PROVIDED]: 
        sort_order = None
    elif args.sortorder == [NOT_PROVIDED] and args.sortcolumn != [NOT_PROVIDED]:
        sort_order = 'asc'
    else:
        sort_order = args.sortorder[0].strip('"').strip()
    
    # Check if --sortorder was provided without --sortcolumn and print a warning
    if sort_order is not None and sort_column is None:
        print(f"{attr.BOLD}{attr.RED}Warning:{attr.END} {attr.BOLD}--sort{attr.END} argument was provided {attr.ITALIC}without{attr.END} {attr.BOLD}--sortcolumn{attr.END}. Sorting will be skipped.\n")
        sort_order = None # Reset sort_order to None to skip sorting
    
    # Process the --chunksize argument, using the default if not provided
    chunk_size = args.chunksize[0] if args.chunksize != [NOT_PROVIDED] else default_chunk_size
    
    # Validate the chunk size to ensure it's within reasonable limits
    if chunk_size < 1 or chunk_size > max_chunk_size:
        print(f"{attr.BOLD}{attr.RED}Error:{attr.END} Chunk size must be between {attr.BOLD}1 and {max_chunk_size}{attr.END}.")
        exit(1)
    
    # Main execution block: call the deduplication function and handle potential errors
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
        print(f"Error details: File: {fname}, Line: {exc_tb.tb_lineno}") # Provide more specific error information
        exit(1)
