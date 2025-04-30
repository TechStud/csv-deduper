
```
   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝
01000011 01010011 01010110  01000100 01000101 01000100 01010101 01010000 01000101 01010010
```


# CSV-DEDUPER

Need to clean up your CSV files from the command-line? `csv-deduper.py` offers a simple solution. `csv-deduper.py` will efficiently remove duplicate rows from a given CSV file, generating a new CSV containing only the unique data you need. 

Using command-line switches you will have the flexibility of specifing particular column(s) for duplicate checking, to select whether to keep the initial or final instance of a duplicate, and to sort your results by a specific column before saving. This tool is powered by the robust `drop_duplicates` functionality of the `pandas` library, ensuring reliable and efficient duplicate removal.

## Installation

Clone, download and/or copy the single `csv-deduper.py` file to your directory/path of choice. Ensure that python and pandas are installed.


## Usage

To use csv-deduper open a terminal and navigate to the directory/path where `csv-deduper.py` is located. NOTE: You can also call it from anywhere (via full/relative path).

	:~$ python3 csv-deduper.py [Options] <file>

### _Examples_:
* Default, command and file, with no options suplied. This will look for identical rows across the entire dataset within the data \<file\> name you provide. 

	- ```:~$ python3 csv-deduper.py <file>```
	- ```:~$ python3 csv-deduper.py ./my-datafile.csv```
	- ```:~$ python3 csv-deduper.py [Options] path/to/my-datafile.csv```

The csv-deduper.py accepts the following Options...

### [OPTIONS]

#### `-c --columns` - If omitted identical rows will be matched. If included, will be used to match the full_column_name(s) (single/multiple) within your CSV file. Replace `"column_name1,column_name2"` with the column/header name(s) you want to use.

  - Dedupe based off a single column_name. No spaces, and no special characters.
```
	:~$ python3 csv-deduper.py -c column_name my-datafile.csv
```
  - Must enclose in single/double quote if spaces/special characters are included in the column name.
```
	:~$ python3 csv-deduper.py -c "column (name)" my-datafile.csv
```
- When two or more columns are referenced, separated by a _comma and no_spaces_. Enclose with single/double quotes is required.
```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" my-datafile.csv
```

#
#### `-k --keep` - Specify whether to keep the 'first' or 'last' occurance of each duplicate matched. Default is 'first'.
```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" -k first my-datafile.csv
```
```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" -k last my-datafile.csv
```

#
#### `-sc --sortcolumn` - After deduping the data, sort your data by a specified column. A single column name only and must enclose in single/double quote if spaces/special characters are included in the column name.

```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" -sc "column_(name2)" my-datafile.csv
```

#
#### `-so --sortorder` - Sort order ('asc' for ascending, 'desc' for descending). Requires '--sortcol'. Single/double quotes are not required.

```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" -sc "column_(name2)" -so asc my-datafile.csv
```
```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" -sc "column_(name2)" -so desc my-datafile.csv
```

#
#### `-ch --chunksize` - Useful for large datasets as it will improve system performance and memory efficiency. Loading a large CSV data file into memory can be impractical or even impossible. The chunksize parameter (a single interger value without comma's) allows you to read the file in smaller, more manageable pieces (chunks). Default is set to 10,000. Max is 500,000. Single/double quotes are not required.

```
	:~$ python3 csv-deduper.py -c "column name1,column_(name2)" -ch 20000 my-data.csv
```

#
#### `-v --version` - show program's version number and exit

```
	:~$ python3 csv-deduper.py -v
```

# Example Outputs
```
:~$ csv-deduper.py ./my-datafile.csv

   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝ v0.2.3
01000011 01010011 01010110 01000100 01000101 01000100 01010101 01010000 01000101 01010010

   Input File : /home/username/scripts/my-datafile.csv 
              : ↳ ~451 Thousand (451,200) rows = 85.78 MiB 
     criteria : ↳ Matching duplicate rows based on all columns.
              : ↳ Keeping the first occurance of any duplicates and dropping the remaining
              : ↳ Final sorting will not be applied
              : ↳ Will iterate through the data using 50,000 row chunksize

 Deduping... [===========================================================>] 100% | Time: 2.04 sec
 ↳ Deduping Process Complete

  Output File : /home/username/scripts/my-datafile_deduped.csv 
              : ↳ ~3 Thousand (2,923) rows = 544.40 KiB
      results : ↳ ~448 Thousand (448,277) rows were removed (99.35%)
              : ↳ Resulting in a 85.25 MiB file reduction (99.38%)
              : ↳ Total processing completed in 2.18 sec
```

```
:~$ ./scripts/csv-deduper.py -c "Brand,Category" -sc "Category" -so desc -ch 10000 ./Downloads/products-1357246.csv

   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝ v0.2.3
01000011 01010011 01010110 01000100 01000101 01000100 01010101 01010000 01000101 01010010

   Input File : /home/techstud/Downloads/products-1357246.csv 
              : ↳ ~1.36 Million (1,357,245) rows | 210.70 MiB 
     criteria : ↳ Matching duplicate rows based on these columns: 'Brand' & 'Category'
              : ↳ Keeping the first occurance of any duplicates and dropping the remaining
              : ↳ Final sorting will be applied to all rows based on 'Category' in desc order
              : ↳ Will iterate through the data using 10,000 row chunksize

 Deduping... [================================================================================>] 100% | Time: 4.80 sec
 ↳ Deduping Process Complete 

  Output File : /home/techstud/Downloads/products-1357246_csv_deduped.csv 
              : ↳ ~1.08 Million (1,083,022) rows | 168.49 MiB 
      results : ↳ ~274 Thousand (274,223) rows were removed (20.20%)
              : ↳ Resulting in a 42.21 MiB file reduction (20.03%)
              : ↳ Total processing completed in 14.83 sec

```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
