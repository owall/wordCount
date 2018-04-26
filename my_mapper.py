#!/usr/bin/python
import sys
import codecs

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, output_stream):
  for line in input_stream.readlines():
    line = line.strip()
    words = line.split()
    for word in words :
      output_stream.write('%s\t%s\n' % (word, 1))
      
def my_map2(input_stream, output_stream):
  wordsCounted = []
  counts = []
  for line in input_stream.readlines():
    line = line.strip()
    words = line.split()
    for word in words :
      if word in wordsCounted :
        idx = wordsCounted.index(word)
        counts[idx] = counts[idx] + 1
      else :
        wordsCounted.append(word)
        counts.append(1)
        
  for i in range(0, len(wordsCounted)) :
    output_stream.write('%s\t%s\n' % (wordsCounted[i], counts[i]))      

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main():
    # We pick the working mode:
    # Mode 1: Debug --> We pick a file to read test the program on it
    #my_input_stream = open("../wiki-filesaa.csv", "r", encoding='utf8')
    #my_output_stream = open("mapResult.txt", "w")

    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    UTF8Reader = codecs.getreader('utf8')
    sys.stdin = UTF8Reader(sys.stdin)
    my_input_stream = sys.stdin

    UTF8Writer = codecs.getwriter('utf8')
    sys.stdout = UTF8Writer(sys.stdout)
    my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    my_main()
