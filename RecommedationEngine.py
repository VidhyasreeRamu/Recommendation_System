#Implemented the Amazon CWBTIAB with two iterations of MapReduce

input  = sc.textFile(“///Users/cd/Desktop/BigData/Assignment4Input.txt”)
input.collect()

# Phase 1: Mapper performs the identity function that emits key = userID, value = item bought by userID

map1 = input.map(lambda x:(x.split(":"))).flatMap(lambda x:[(x[0],x[1])]).distinct()
map1.collect()

# Reducer1 – groups all items for a single user .This is implemented by using groupByKey and mapValues. key = userID & value = list of items bought by userID

r1 = map1.groupByKey().mapValues(lambda x:list(x))
r1.collect()

# Mapper – Phase 2: Used the Stripes design pattern. It generates the list of Stripes (represented as dict) along with frequency for each item bought by user. The output of this mapper is key = item value = list of stripes[D1, D2, ..., Dn]

def mapper(kv):
   	     items = kv[1]
   	     stripes = dict()
   	     for i in items:
   	         map = dict()
   	         for j in items:
   	             if j is not i:
   	                 if j in map:
   	                     map[j]= map[j]+1
   	                 else:
   	                     map[j] = 1
   	         stripes [i]=map
   	     kv =[(x,stripes[x]) for x in stripes]
   	     return kv

map2 = r1.flatMap(mapper)
map2.collect()

#Reducer Phase 2: Performs an item-wise sum of all stripes for a given item. It is ordered in descending order of frequency of co occurence
red2=map2.reduceByKey(lambda x,y:OrderedDict(sorted(dict(Counter(x)+Counter(y)).items(), key=lambda t:-t[1] )))

red2.mapValues(lambda x:list(x.items())).collect()

#Result: Returns 3 items, representing the top or maximum frequencies of co occurence.

result = red2.mapValues(lambda x:list(x.items())[:3])
