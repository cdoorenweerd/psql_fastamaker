#/usr/bin/env python
# Camiel Doorenweerd 2026
# The script needs a .connectstring_databasealias file with SQL connection info in SQLalchemy format

import os
import pandas as pd
import csv
import psycopg2
import argparse
import sys
import time
import linecache
import subprocess
from sqlalchemy import create_engine
from shutil import which
from pathlib import Path

parser = argparse.ArgumentParser(description="A script to pull sequences from the psql database with the latest identifications and write to FASTA. A .connectstring_databasealias file with the password to access the database is required in the root of where the script is executed." )
parser.add_argument("-d", "--database", metavar="", required=True,
                    help="Alias of the database to connect to, via the '.connectstring_databasealias' file")
parser.add_argument("-m", "--markerset", metavar="", required=True,
                    help="Name of the markerset, must match database exactly (use -l to see list of options)")
parser.add_argument("-n", "--naming", metavar="", default="classic", 
                    help="Sequence identifier naming convention, 'classic' (default), 'camiel', 'barcodingr', 'bold', 'pycoistats' or 'monophylizer'")
parser.add_argument("-a", "--align", metavar="", default="yes", 
                    help="Align sequences using MAFFT, yes (default) or no")
parser.add_argument("-w", "--wishlist", metavar="", default="nolist", 
                    help="List with sampleID's to select, in .csv format")
args = parser.parse_args()


newname = args.naming
wishlistcsv = args.wishlist
database = args.database
markerset = args.markerset
align = args.align
os.makedirs('output_alignments', exist_ok=True)
datetoday = time.strftime("%Y%m%d")

def psqlconnectiontest(database):
    connectstringfile = f"./.connectstring_{database}"
    if not os.path.exists(connectstringfile):
        sys.exit(f"Missing .connectstring file for database {database} : stopping")
    connectstring = linecache.getline(connectstringfile, 1).rstrip('\n')
    try:
        engine = create_engine(connectstring)
        with engine.connect() as conn:
            print(f"Successfully connected to psql database: {database}")
        return engine # return engine so you can pass it directly to pd.read_sql_query()

    except Exception as e:
        sys.exit(f"Could not connect to psql database: stopping\nError: {e}")


def markersetcheck(engine, markerset):
    sql = "SELECT markerset FROM dna_markersets;"
    df_markersetoverview = pd.read_sql_query(sql, engine)
    conn = None
    if markerset not in df_markersetoverview.values:
        print("Your selected markerset is invalid, pick one of the following:")
        print(df_markersetoverview)
        sys.exit()


def getmarkerlist(engine, markerset):
    sql = "SELECT markers FROM dna_markersets WHERE markerset = '" + markerset + "';"
    df_markerset = pd.read_sql_query(sql, engine)
    conn = None
    markerlist = list(map(str, df_markerset['markers'][0].split(",")))
    return markerlist


def makefasta(df, marker):
    """
    Generates a FASTA file from a pandas DataFrame and optionally aligns it using MAFFT.
    """
    if df.empty:
        print("Could not find any sequences")
        return
    outputname = f"{datetoday}_{database}_{marker}.fas"
    outputpathname = os.path.join('output_alignments', outputname)
    tmpname = '.tmpunaligned.fas'
    seq_count = len(df)
    
    # 1. Handle unaligned fasta output directly
    if align == "no":
        with open(outputpathname, 'w') as fasta_output: 
            for row in df.itertuples():
                header = str(getattr(row, newname)).replace(" ", "_")
                fasta_output.write(f">{header}\n{row.seq}\n")
        print(f"Found {seq_count} sequence(s) of {marker}")
        print(f"Created {outputname}")
        
    # 2. Handle aligned fasta output using MAFFT
    elif align == "yes":
        if which("mafft") is None:
            print("Could not find a MAFFT executable, exiting")
            sys.exit()
        with open(tmpname, 'w') as fasta_output:
            for row in df.itertuples():
                header = str(getattr(row, newname)).replace(" ", "_")
                fasta_output.write(f">{header}\n{row.seq}\n")
        print(f"Found {seq_count} sequence(s) of {marker}")
        print("Aligning with MAFFT...")
        with open(outputpathname, 'w') as out_file:
            subprocess.run(["mafft", "--auto", "--quiet", tmpname], stdout=out_file, check=True)
        if os.path.exists(tmpname):
            os.remove(tmpname)
        print(f"Created {outputname}")


def lookupseqs(engine, markerlist, wishlistcsv):
    if wishlistcsv == "nolist":
        for marker in markerlist:
            sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "';"
            df = pd.read_sql_query(sql, engine)
            conn = None
            makefasta(df, marker)
    else:
        with open(wishlistcsv, "r", encoding="utf8") as wishlistfile: 
            reader = csv.reader(wishlistfile)
            wishlist = list(reader)
            flatwishlist = [name for sublist in wishlist for name in sublist]
            flatwishliststring = str(flatwishlist).replace("[", "").replace("]", "")
            print("Looking up sequences for " + str(len(flatwishlist)) + " unique ID's in " + str(len(markerlist)) + " marker(s)")
        for marker in markerlist:
            sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "' AND extractcode IN (" + flatwishliststring + ");"
            df = pd.read_sql_query(sql, engine)
            conn = None
            makefasta(df, marker)


# test connection with database and return SQLAlchemy connection engine if successful
engine = psqlconnectiontest(database)

# check is selected markerset is available or give list of which ones are
markersetcheck(engine, markerset)

# create a list of the markers requested
markerlist = getmarkerlist(engine, markerset)

# find the sequences for each requested marker and optionally wishlist, optionally align, and write into FASTA file
lookupseqs(engine, markerlist, wishlistcsv)