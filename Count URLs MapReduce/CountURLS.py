# Author: Trudy Howles
#modified by: Shraddha Atrawalkar (ssa2923)
#             Kumar Abhishek (kxa7954)
# This python code computes how many URLs in the indicated file were accessed more than 20 times?

import MapReduce
import sys
import re

#create an object of the MapReduce class
mr = MapReduce.MapReduce()

#create a variable to store the count of URLs
threatcount=0

#define the mapper method
def mapper (data):
 #extract the substring with url from each line
 link = re.search('"(.+?)HTTP/', data)

 #if the substring is found, extract the URL and add to the intermediate list using the emit_intermediate function
 if link:
    extract = link.group(1)
    pos = extract.index(" ")
    length = len (extract)
    url = extract[pos+1:length]
    mr.emit_intermediate (url, 1)

#define the reducer method    
def reducer (key, list_of_values):
#define a global variable to get the URLs that are accessed more than 20 times
   global threatcount
 #get a count of how many times the url(key) was accessed
   value = len (list_of_values)

 #if the count is greater than 20, increment the threat count
   if value > 20:
      threatcount=threatcount+1

def threatURL():
   mr.emit(threatcount)
   print(threatcount," URLs were accessed more than 20 times")

if __name__ == '__main__':
   #call the execute method from the MapReduce class
    mr.execute(sys.argv[1], mapper, reducer)
    #print the count
    threatURL()
                                                  
