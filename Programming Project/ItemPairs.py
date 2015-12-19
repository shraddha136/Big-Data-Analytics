#***********************************************#
# Authors: Shraddha Atrawalkar (ssa2923)
#          Kumar Abhishek (kxa7954)
# This code prints all the item pairs that 
# occur more than 650 times in the list of 
# transactions provided in the file
#***********************************************#



import re
import sys
import MapReduce

#create an object of the MapReduce class
mr = MapReduce.MapReduce()

#define the mapper function which is called by passing each transaction
def mapper (data):
    
    #define indicators between which items are found
    i = ","
    j = "]"
    
    #extract the list of items present in the transaction;
    #this is done to exclude the transaction number from the data
    itemlist = re.search('%s(.*)%s' % (i, j), data).group(1)
    
    #get each item from the list above
    items = itemlist.split(" ")
    
    #sort the items alphabetically
    items.sort()
    
    #generate pairs of items that occurred in the transaction
    for ind1, item in enumerate(items):
        if len(item) > 1:
            for ind2, item2 in enumerate(items):
                if len(item2) > 1 and item != item2 and ind2 > ind1 :
                    #create a pair of items separated by /
                    pair = item + "/" +item2
                    #add the pair to the intermediate dictionary of the MapReduce class
                    mr.emit_intermediate(pair, 1)
            
            
#define the reduce function            
def reducer (key, list_of_values):
    #get the count of occurrences of the item pairs
    value = len(list_of_values)
    
    #if this pair has occurred frequently i.e. more than 650 times, send it to the reducer
    if value > 650:
        mr.emit(key)
  
if __name__ == '__main__':
    #call the execute method from the MapReduce class
    mr.execute(sys.argv[1], mapper, reducer)












