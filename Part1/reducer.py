#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_file_name = None
current_count = 0
word = None
fileName = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    part1, count = line.split('\t', 1)
    word, fileName = part1.split(":", 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word and current_file_name == fileName:
        current_count += count
    else:
        if current_word and current_file_name:
            # write result to STDOUT
            print(current_word, ":", current_file_name,  '\t', current_count)
        current_count = count
        current_word = word
        current_file_name = fileName

# do not forget to output the last word if needed!
if current_word == word:
    print(current_word, '\t', current_count)
