#/usr/bin/env python
#Camiel Doorenweerd 2019

import pandas as pd
import psycopg2
import argparse

parser = argparse.ArgumentParser(description="A script to pull sequences from the psql database with the latest identifications and write to FASTA. A .pgpass file with the password to enter the database is required." )
parser.add_argument("-o", "--outputfile", metavar="", 
                    help="Output file name")
parser.add_argument("-m", "--marker", metavar="", 
                    help="Name of the marker, must match database exactly")
parser.add_argument("-l", "--list", action="store_true", 
                    help="Gives a list of the markers in the database")
args = parser.parse_args()


outputname = args.outputfile
marker = args.marker
listrequest = args.list

def producemarkerlist():
    conn = psycopg2.connect("host=localhost dbname=fruitfly user=postgres")
    sql = "SELECT marker, COUNT(marker) FROM dnaseqs GROUP BY marker;"
    df_markerlist = pd.read_sql_query(sql, conn)
    print(df_markerlist)
    conn = None


def makefasta(marker,outputname):
    conn = psycopg2.connect("host=localhost dbname=fruitfly user=postgres")
    sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "';"
    df = pd.read_sql_query(sql, conn)
    conn = None
    
    with open(outputname, 'a') as fasta_output:
        for index, row in df.iterrows():
            fasta_output.write('>' + (str(row['newname'])).replace(" ","_")
                               + '\n'
                               + str(row['seq']) + '\n' + '\n')

if listrequest == True:
    producemarkerlist()
else:
    makefasta(marker,outputname)