#/usr/bin/env python
#Camiel Doorenweerd 2019
# The script will assume there is a .connectstring_databasealias file in the root with a line like localhost:5432:fruitfly12_brew:postgres:password

import pandas as pd
import csv
import psycopg2
import argparse
import sys
import time
import os.path
import linecache
from pathlib import Path

parser = argparse.ArgumentParser(description="A script to pull sequences from the psql database with the latest identifications and write to FASTA. A .connectstring_databasealias file with the password to access the database is required in the root of where the script is executed." )
parser.add_argument("-d", "--database", metavar="", required=True,
                    help="Alias of the database to connect to, via the '.connectstring_databasealias' file")
parser.add_argument("-s", "--markerset", metavar="", required=True,
                    help="Name of the markerset, must match database exactly (use -l to see list of options)")
parser.add_argument("-n", "--name", metavar="", default="classic", 
                    help="Sequence identifier naming convention, 'classic' (default), 'barcodingr', 'bold', 'pycoistats' or 'monophylizer'")
parser.add_argument("-w", "--wishlist", metavar="", default="nolist", 
                    help="List with sampleID's to select, in .csv format")
args = parser.parse_args()


newname = args.name
wishlistcsv = args.wishlist
database = args.database
markerset = args.markerset
Path("./output_alignments").mkdir(parents=True, exist_ok=True)
datetoday = time.strftime("%Y%m%d")

connectstringfile = str('.connectstring_' + database)
if os.path.exists('./.connectstring_' + database) == False:
    sys.exit("Missing .connectstring file: stopping")
connectstring = linecache.getline(filename=connectstringfile, lineno=1)
conn = psycopg2.connect(connectstring)
if conn.closed == 0:
    print("Successfully connected to psql database")
else:
    sys.exit("Could not connect to psql database: stopping") 
conn = None


def getmarkerlist(markerset):
    conn = psycopg2.connect(connectstring)
    sql = "SELECT markers FROM markersets WHERE markerset = '" + markerset + "';"
    df_markerset = pd.read_sql_query(sql, conn)
    conn = None
    markerlist = list(map(str, df_markerset['markers'][0].split(",")))
    return markerlist


def makefasta(markerlist):
    for marker in markerlist:
        conn = psycopg2.connect(connectstring)
        sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "';"
        df = pd.read_sql_query(sql, conn)
        conn = None

        outputname = datetoday + '_' + marker + '.fas'
        outputpathname = 'output_alignments/' + outputname
        with open(outputpathname, 'a') as fasta_output:
            for index, row in df.iterrows():
                fasta_output.write('>' + (str(row[newname])).replace(" ","_")
                                    + '\n'
                                    + str(row['seq']) + '\n')
            print("Created " + str(outputname))


def makeselectedfasta(markerlist,wishlistcsv):
    with open(wishlistcsv, "r", encoding="utf8") as wishlistfile: 
        reader = csv.reader(wishlistfile)
        wishlist = list(reader)
        flatwishlist = [name for sublist in wishlist for name in sublist]
        flatwishliststring = str(flatwishlist).replace("[", "").replace("]", "")
        print("Looking up sequences for " + str(len(flatwishlist)) + " unique ID's in " + str(len(markerlist)) + " markers")

    for marker in markerlist:
        conn = psycopg2.connect(connectstring)
        sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "' AND mscode IN (" + flatwishliststring + ");"
        df = pd.read_sql_query(sql, conn)
        conn = None

        if not df.empty:
            outputname = datetoday + '_' + marker + '.fas'
            outputpathname = 'output_alignments/' + outputname
            with open(outputpathname, 'a') as fasta_output:
                for index, row in df.iterrows():
                    fasta_output.write('>' + (str(row[newname])).replace(" ","_")
                                        + '\n'
                                        + str(row['seq']) + '\n')
                print("Created " + str(outputname) + " with selected sequences.")


markerlist = getmarkerlist(markerset)

if wishlistcsv != "nolist":
    makeselectedfasta(markerlist,wishlistcsv) 
else:
    makefasta(markerlist)