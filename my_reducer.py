#Assuming only one reducer


# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, output_stream):

    current_word = None
    current_count = 0
    word = None
    
    topHundredWords = {}
    
    for line in input_stream.readlines() :
      line = line.strip()
      word, count = line.split('\t', 1)
      
      try:
        count = int(count)
      except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
      
      # this IF-switch only works because Hadoop sorts map output
      # by key (here: word) before it is passed to the reducer
      if current_word == word:
          current_count += count
      else:
          if current_word:
            if len(topHundredWords) < 100 :
              topHundredWords[current_word] = current_count
            else :
              key_min_value = min(topHundredWords, key=topHundredWords.get)
              if current_count > topHundredWords[key_min_value] :
                del topHundredWords[key_min_value] 
                topHundredWords[current_word] = current_count
              #find the word with min value
              #delete it and insert current_word
            # output_stream.write('%s\t%s\n' % (current_word, current_count))
          current_count = count
          current_word = word
          
    if current_word == word:
      if len(topHundredWords) < 100 :
        topHundredWords[current_word] = current_count
      else :
        key_min_value = min(topHundredWords, key=topHundredWords.get)
        if current_count > topHundredWords[key_min_value] :
          del topHundredWords[key_min_value] 
          topHundredWords[current_word] = current_count
          
    sorted_list = [(k,v) for v,k in sorted([(v,k) for k,v in topHundredWords.items()], reverse=True)]

    for word, count in sorted_list:
      output_stream.write('%s\t%s\n' % (word, count))
    output_stream.close()
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main():
    # We pick the working mode:
    # Mode 1: Debug --> We pick our file to read test the program on it
    my_input_stream = open("sort_simulation.txt", "r", encoding='utf8')
    my_output_stream = open("reduce_simulation.txt", "w", encoding='utf8')

    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
#    UTF8Reader = codecs.getreader('utf8')
#    sys.stdin = UTF8Reader(sys.stdin)
#    my_input_stream = sys.stdin
#
#    UTF8Writer = codecs.getwriter('utf8')
#    sys.stdout = UTF8Writer(sys.stdout)
#    my_output_stream = sys.stdout

    # We launch the Map program
    my_reduce(my_input_stream, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    my_main()
