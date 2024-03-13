# About

Python scripts to pull DNA sequence data from a postgreSQL database, optionally aligned.

Scripts included in this repository:

### psql_fastamaker.py

A python script to generate a FASTA file from sequences stored in a postgresql database. Arguments:

- ```d --database```. Name of the postgresql database. The script exepcts a '.connectstring_databasealias' file in the folder where it is run with the database name, address, port, username, and password. The string is not parsed but will be copied to connect to the database.

- ```-m --markerset```. Our postgresql database has several markersets defined (e.g., COI, HiMAP500) to pull multiple markers with a single command for the same group of taxa. Use ```'-l'``` to get a list of the available options.

- ```-n --naming```. Optional, select the naming convention as defined in the postgresql database, e.g., bold, barcodingr, pycoistats, etc. Default "classic".

- ```-a --align```. Optional, yes or no (default), when 'yes', will use MAFFT from the path to align sequences per marker. Assumes MAFFT is installed; recommended installation method is with conda.

- ```-w --wishlist```. Optional, provide a list with specimen identifiers for which sequences will be looked up. One line per identifier.

### fastas_to_spreadsheet.py

This script is mainly used to add new sequences to the postgresql database by converting a fasta file to a csv and excel spreadsheet. It will include all .fas and .fasta files in its current folder. This will only work on fasta failes that have a single line per sequence. Optional arguments:

- ```-t --trimN```. Removes 'N's from the sequence when given.
- ```-g --gaps```. Removes '-'s from the sequence when given.


### AMAS.py

This script is included here for convenience, it is a copy from https://github.com/marekborowiec/AMAS . It is useful for concatenating multiple alignments and creating partition files for phylogenetic analysis.