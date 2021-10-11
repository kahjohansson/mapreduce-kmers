#!/usr/bin/python3
import sys


def main(k=9, separator='\t'):
    """ Maps all of the k-mers from input

    Parameters
    ----------
    k : size of the subsequence of nucleotides, the k-mer
    separator : ouput separator, that is placed between k-mer and value count"""
    string = sys.stdin.readlines()[0].strip()
    n = len(string)
    for i in range(n-k+1):
        print (f'{string[i:i+k]}{separator}1')


if __name__ == "__main__":
    main()
