#!/usr/bin/python3
from itertools import groupby
from operator import itemgetter
import sys


def read_mapper_output(file, separator='\t'):
    """ Reads the mapper output and yields each k-mer
    
    Parameters
    ----------
    file : mapper's outfile file
    separator : separator between value and count """
    
    for line in file:
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    """ Reduces the occurrences of each value and prints its count
    
    Parameters
    ----------
    separator : separator between value and count """
    data = read_mapper_output(sys.stdin, separator=separator)
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print(f'{current_word}{separator}{total_count}') 
        except ValueError:
            pass


if __name__ == "__main__":
    main()
