#/usr/bin/env python
#Camiel Doorenweerd 2019
# The script will assume there is a .pgpass file in the root with a line like localhost:5432:fruitfly12_brew:postgres:password

import pandas as pd
import csv
import psycopg2
import argparse

parser = argparse.ArgumentParser(description="A script to pull sequences from the psql database with the latest identifications and write to FASTA. A .pgpass file with the password to access the database is required." )
parser.add_argument("-o", "--outputfile", metavar="", 
                    help="Output file name")
parser.add_argument("-m", "--marker", metavar="", 
                    help="Name of the marker, must match database exactly (use -l for list of options)")
parser.add_argument("-n", "--name", metavar="", default="classic", 
                    help="Sequence identifier naming convention, 'classic' (default), 'barcodingr', 'bold' or 'monophylizer'")
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
connectstring = "host=localhost dbname=fruitfly12_brew user=postgres port=5432"


def producemarkerlist():
    conn = psycopg2.connect(connectstring)
    sql = "SELECT marker, COUNT(marker) FROM dnaseqs GROUP BY marker;"
    df_markerlist = pd.read_sql_query(sql, conn)
    print(df_markerlist)
    conn = None


def makefasta(marker,outputname):
    conn = psycopg2.connect(connectstring)
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
        print("Looking up sequences for: " + flatwishliststring)

    conn = psycopg2.connect(connectstring)
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