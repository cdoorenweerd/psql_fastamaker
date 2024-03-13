#/usr/bin/env python
# Camiel Doorenweerd 2024

import pandas as pd
import argparse
import glob

parser = argparse.ArgumentParser(description="Script to turn all fasta files (.fas and .fasta) in current folder into a single csv and xls. Requires single line sequence fastas.")
parser.add_argument("-t", "--trimN", default=False, action="store_true",
                    help="Removes completely ambiguous bases 'N'")
parser.add_argument("-g", "--gaps", default=False, action="store_true",
                    help="Removes gaps '-'")
args = parser.parse_args()

fastas = glob.glob('./*.fas') + glob.glob('./*.fasta')
print("Parsing and combining " + str(len(fastas)) + " fasta files. Files included:")
print(fastas)

seqs = []
for fasta in fastas:
    with open(fasta) as ifile:
        for line in ifile:
            if line.startswith(">"):
                dnasequence = str(next(ifile, '').upper())
                if args.trimN: dnasequence = dnasequence.replace('N','')
                if args.gaps: dnasequence = dnasequence.replace('-','')
                seqs.append({'sourcefile': fasta,
                             'seqname': line.replace('>',''),
                             'seq': dnasequence})

df = pd.DataFrame(seqs)
df.to_csv('test.csv', index=False)
df.to_excel('test.xlsx', index=False)