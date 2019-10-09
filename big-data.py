## Big Data
%%writefile example2.txt
first 
second line
the third line
then a fourth line

from pyspark import SparkContext

sc = SparkContext()

# Show RDD
sc.textFile('example2.txt')

# Save a reference to this RDD
text_rdd = sc.textFile('example2.txt')

# Map a function (or lambda expression) to each line
# Then collect the results.
text_rdd.map(lambda line: line.split()).collect()

## Map vs flatMap

# Collect everything as a single flat map
text_rdd.flatMap(lambda line: line.split()).collect()

# RDDs and Key Value Pairs

%%writefile services.txt
#EventId    Timestamp    Customer   State    ServiceID    Amount
201       10/13/2017      100       NY       131          100.00
204       10/18/2017      700       TX       129          450.00
202       10/15/2017      203       CA       121          200.00
206       10/19/2017      202       CA       131          500.00
203       10/17/2017      101       NY       173          750.00
205       10/19/2017      202       TX       121          200.00

services = sc.textFile('services.txt')

services.take(2)

services.map(lambda x: x.split())

services.map(lambda x: x.split()).take(3)

Let's remove that first hash-tag!

services.map(lambda x: x[1:] if x[0]=='#' else x).collect()

services.map(lambda x: x[1:] if x[0]=='#' else x).map(lambda x: x.split()).collect()

## Using Key Value Pairs for Operations

# From Previous
cleanServ = services.map(lambda x: x[1:] if x[0]=='#' else x).map(lambda x: x.split())

cleanServ.collect()

# Let's start by practicing grabbing fields
cleanServ.map(lambda lst: (lst[3],lst[-1])).collect()

# Continue with reduceByKey
# Notice how it assumes that the first item is the key!
cleanServ.map(lambda lst: (lst[3],lst[-1]))\
         .reduceByKey(lambda amt1,amt2 : amt1+amt2)\
         .collect()

# Continue with reduceByKey
# Notice how it assumes that the first item is the key!
cleanServ.map(lambda lst: (lst[3],lst[-1]))\
         .reduceByKey(lambda amt1,amt2 : float(amt1)+float(amt2))\
         .collect()

We can continue our analysis by sorting this output:

# Grab state and amounts
# Add them
# Get rid of ('State','Amount')
# Sort them by the amount value
cleanServ.map(lambda lst: (lst[3],lst[-1]))\
.reduceByKey(lambda amt1,amt2 : float(amt1)+float(amt2))\
.filter(lambda x: not x[0]=='State')\
.sortBy(lambda stateAmount: stateAmount[1], ascending=False)\
.collect()

** Remember to try to use unpacking for readability. For example: **

x = ['ID','State','Amount']

def func1(lst):
    return lst[-1]

def func2(id_st_amt):
    # Unpack Values
    (Id,st,amt) = id_st_amt
    return amt

func1(x)

func2(x)
