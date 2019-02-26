#/usr/bin/env python
#Camiel Doorenweerd 2019

import pandas as pd
import psycopg2
import argparse

parser = argparse.ArgumentParser(description="A script to pull sequences from the psql database with the latest identifications and write to FASTA. A .pgpass file with the password to enter the database is required." )
parser.add_argument("-o", "--outputfile", metavar="", required=True,
                    help="Output file name")
parser.add_argument("-m", "--marker", metavar="", 
                    help="Name of the marker, must match database exactly")
args = parser.parse_args()


outputname = args.outputfile
marker = args.marker


#conn_string = "host='localhost' dbname='fruitfly' user='postgres' password='mypassword'"
conn = psycopg2.connect("host=localhost dbname=fruitfly user=postgres")
sql = "SELECT * FROM renamed_seqs WHERE marker = '" + marker + "';"
df = pd.read_sql_query(sql, conn)
conn = None

with open(outputname, 'a') as fasta_output:
    for index, row in df.iterrows():
        fasta_output.write('>' + str(row['newname']) + '\n'
                           + str(row['seq']) + '\n' + '\n')