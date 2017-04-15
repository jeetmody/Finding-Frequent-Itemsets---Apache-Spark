from pyspark import SparkContext
from collections import defaultdict
from itertools import combinations
import time



from operator import add
import sys

sc = SparkContext(appName="inf553")
#Start code

def findCount(x,query):
    count = 0
    for i in x:
        
        if(set(query).issubset(i)):
            count+=1
    return count


def apriori(x,sup):
    x = list(x)
    #print x
 
    d1={}
    #sup = 0.3
    for i in x:
        for j in i:
            if(j in d1):
                d1[j]+=1
            else:
                d1[j]=1
    supCount = sup*len(x)
    tempList=set()
    finalList=[]
    for i in d1:
        if(d1[i]>=supCount):
            finalList.append(i)
            tempList.add(i)
    curSize = 2
    candidates = tempList
    while(len(candidates)!=0):
        newPairs=[]
        k=0
        for p in candidates:
            for q in candidates:
                if(curSize>2):
                    i=set(p)
                    j=set(q)
                else:
                    i = set()
                    j = set()
                    i.add(p)
                    j.add(q)
                uni = i.union(j)
                sort1 = tuple(sorted(tuple(uni)))
                if(sort1 not in finalList):
                    if(len(uni)==curSize):
                        flag = 0
                        if(curSize!=2):
                            y = list(combinations(uni,curSize-1))
                            flag = 0
                            for z in y:
                                if(tuple(sorted(tuple(z))) not in finalList):
                                    flag = 1
                                    break
                        if(flag == 0):
                            if(findCount(x,uni)>=supCount):
                                newPairs.append(uni)
                                finalList.append(sort1)
    
        curSize+=1
        candidates = newPairs
    return tuple(finalList)

def func1(x):
    return 1

def func2(x):
    return (x,1)

def countCheck(x,pairs):
    retList=[]
    x = list(x)
    tempSet = set()
    count =0
    for i in pairs:
        if(type(i[0])==unicode):
            temp = set()
            count+=1
            temp.add(i[0])
        else:
            temp = set(i[0])
        retList.append([i[0],findCount(x,temp)])
    return retList


def main():
    
    start_time = time.time()

    
    inputFile = sys.argv[1]
    sup = float(sys.argv[2])
    output = sys.argv[3]
    rdd = sc.textFile(inputFile).map(lambda x:x.split(','))
    size = len(rdd.collect())
    rdd2 = rdd.mapPartitions(lambda x:apriori(x,sup)).map(lambda x:(x,1)).reduceByKey(lambda x,y:1)
    locaLFrequent1 =  rdd2.collect() #all frequent pairs
    supCount=sup*size
    rdd3 = rdd.mapPartitions(lambda x: countCheck(x,locaLFrequent1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=supCount)
    #print "ANSWER----"
    answer = rdd3.collect()
    file = open(output,'w')
    for val in answer:
        if(type(val[0])==unicode):
            for i in range(len(val[0])):
                op = str(val[0][i])
                file.write("%s"%op)
        else:
            for i in range(len(val[0])):
                op = str(val[0][i])
                if(i!=(len(val[0])-1)):
                    file.write("%s,"%op)
                else:
                    file.write("%s"%op)
        file.write("\n")
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
