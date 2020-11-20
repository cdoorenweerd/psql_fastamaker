#/usr/bin/env python
#Camiel Doorenweerd 2019
# The script will assume there is a .connectstring_databasealias file in the root with a line like localhost:5432:fruitfly12_brew:postgres:password

import pandas as pd
import csv
import psycopg2
import argparse
import sys
import os.path
import linecache
from pathlib import Path

parser = argparse.ArgumentParser(description="A script to pull sequences from the psql database with the latest identifications and write to FASTA. A .connectstring_databasealias file with the password to access the database is required in the root of where the script is executed." )
parser.add_argument("-o", "--outputfile", metavar="", 
                    help="Output file name")
parser.add_argument("-d", "--database", metavar="", 
                    help="Alias of the database to connect to, via the '.connectstring_databasealias' file")
parser.add_argument("-m", "--marker", metavar="", 
                    help="Name of the marker, must match database exactly (use -l to see list of options)")
parser.add_argument("-n", "--name", metavar="", default="classic", 
                    help="Sequence identifier naming convention, 'classic' (default), 'barcodingr', 'bold', 'pycoistats' or 'monophylizer'")
parser.add_argument("-w", "--wishlist", metavar="", default="nolist", 
                    help="List with sampleID's to select, in .csv format")
parser.add_argument("-l", "--list", action="store_true", 
                    help="Prints a list of the markers in the database to screen")
args = parser.parse_args()


outputname = args.outputfile
marker = args.marker
listrequest = args.list
newname = args.name
wishlistcsv = args.wishlist
database = args.database

connectstringfile = str('.connectstring_' + database)
if os.path.exists('./.connectstring_' + database) == False:
    sys.exit("Missing .connectstring file: stopping")
connectstring = linecache.getline(filename=connectstringfile, lineno=1)

def producemarkerlist():
    conn = psycopg2.connect(connectstring)
    if conn.closed == 0:
           print("Successfully connected to psql database")
    else:
           sys.exit("Could not connect to psql database: stopping")    
    sql = "SELECT marker, COUNT(marker) FROM renamed_seqs GROUP BY marker;"
    df_markerlist = pd.read_sql_query(sql, conn)
    print(df_markerlist)
    conn = None


def makefasta(marker,outputname):
    conn = psycopg2.connect(connectstring)
    if conn.closed == 0:
           print("Successfully connected to psql database")
    else:
           sys.exit("Could not connect to psql database: stopping") 
    sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "';"
    df = pd.read_sql_query(sql, conn)
    conn = None
    
    with open(outputname, 'a') as fasta_output:
        for index, row in df.iterrows():
            fasta_output.write('>' + (str(row[newname])).replace(" ","_")
                                + '\n'
                                + str(row['seq']) + '\n')
        print("Created " + str(outputname))


def makeselectedfasta(marker,outputname,wishlistcsv):
    with open(wishlistcsv, "r", encoding="utf8") as wishlistfile: 
        reader = csv.reader(wishlistfile)
        wishlist = list(reader)
        flatwishlist = [name for sublist in wishlist for name in sublist]
        flatwishliststring = str(flatwishlist).replace("[", "").replace("]", "")
        print("Looking up sequences for " + str(len(flatwishlist)) + " unique ID's.")

    conn = psycopg2.connect(connectstring)
    if conn.closed == 0:
           print("Successfully connected to psql database")
    else:
           sys.exit("Could not connect to psql database: stopping") 
    sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "' AND mscode IN (" + flatwishliststring + ");"
    df = pd.read_sql_query(sql, conn)
    conn = None

    with open(outputname, 'a') as fasta_output:
        for index, row in df.iterrows():
            fasta_output.write('>' + (str(row[newname])).replace(" ","_")
                                + '\n'
                                + str(row['seq']) + '\n')
        print("Created " + str(outputname) + " with selected sequences.")


if listrequest == True:
    producemarkerlist()
elif wishlistcsv != "nolist":
    makeselectedfasta(marker,outputname,wishlistcsv) 
else:
    makefasta(marker,outputname)