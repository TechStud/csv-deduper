
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

Using command-line switches you will the flexibility of specifing particular column(s) for duplicate checking, to select whether to keep the initial or final instance of a duplicate, and to sort your results by a specific column before saving. This tool is powered by the robust `drop_duplicates` functionality of the `pandas` library, ensuring reliable and efficient duplicate removal.

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

# Example Outputs
```
:~$ python3 csv-deduper.py ./my-datafile.csv

   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝
01000011 01010011 01010110 01000100 01000101 01000100 01010101 01010000 01000101 01010010

 Processing... [##################################################] 100% | Time: 2.04 sec | [451,200/451,200]

 [ Processing Complete ]
   Input File : /home/username/scripts/my-datafile.csv 
              : ↳ 451,200 rows = 85.78 MB 
     criteria : ↳ Match duplicate rows based on all columns.
              : ↳ Keep the first occurance of any duplicates and drop the remaining
              : ↳ Final sorting not applied

  Output File : /home/username/scripts/my-datafile_deduped.csv 
              : ↳ 2,923 rows = 544.40 KB 
              : ↳ 448,277 rows were Deduped (99.35%)
              : ↳ Resulting in a 85.25 MB file Reduction (99.38%)
              : ↳ Processing completed in 2.18 sec
```

```
:~$ python3 csv-deduper.py -c "Time_(UTC),Item_Number" -sc "Time_(UTC)" -so desc -ch 50000 ./data/my-datafile.csv

   ██████╗███████╗██╗   ██╗    ██████╗ ███████╗██████╗ ██╗   ██╗██████╗ ███████╗██████╗ 
  ██╔════╝██╔════╝██║   ██║    ██╔══██╗██╔════╝██╔══██╗██║   ██║██╔══██╗██╔════╝██╔══██╗
  ██║     ███████╗██║   ██║    ██║  ██║█████╗  ██║  ██║██║   ██║██████╔╝█████╗  ██████╔╝
  ██║     ╚════██║╚██╗ ██╔╝    ██║  ██║██╔══╝  ██║  ██║██║   ██║██╔═══╝ ██╔══╝  ██╔══██╗
  ╚██████╗███████║ ╚████╔╝     ██████╔╝███████╗██████╔╝╚██████╔╝██║     ███████╗██║  ██║
   ╚═════╝╚══════╝  ╚═══╝      ╚═════╝ ╚══════╝╚═════╝  ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝
01000011 01010011 01010110 01000100 01000101 01000100 01010101 01010000 01000101 01010010

 Processing... [##################################################] 100% | Time: 1.79 sec | [451,200/451,200]

 [ Processing Complete ]
   Input File : /home/username/scripts/data/my-datafile.csv 
              : ↳ 451,200 rows = 85.78 MB 
     criteria : ↳ Match duplicate rows based on these columns: 'Time_(UTC)' & 'IP_Address'
              : ↳ Keep the first occurance of any duplicates and drop the remaining
              : ↳ Final sorting applied to all rows based on 'Time_(UTC)' in desc order

  Output File : /home/username/scripts/data/my-datafile_csv_deduped.csv 
              : ↳ 1,382 rows = 544.40 KB 
              : ↳ 449,818 rows were Deduped (99.69%)
              : ↳ Resulting in a 85.25 MB file Reduction (99.38%)
              : ↳ Processing completed in 1.81 sec
              :   ↳ Using default ChunkSize: 50,000 lines

```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
