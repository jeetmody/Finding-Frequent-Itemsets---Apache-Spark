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
    #print "Support"
    #print supCount
    #print d1
    #print "Done"
    #return d1
    tempList=set()
    finalList=[]
    for i in d1:
        if(d1[i]>=supCount):
            finalList.append(i)
            tempList.add(i)

    #print "TempList :"
    #print tempList
    curSize = 2
    candidates = tempList
    #print candidates
    #print len(candidates)
    while(len(candidates)!=0):
        newPairs=[]
        k=0
        #print "hello"
        #i = set()
        #j = set()
        for p in candidates:
            for q in candidates:
                

                #print "hi"
                #print p
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
                #i.add(p)
                #print "i:",
                #print i
                #j.add(q)
                if(sort1 not in finalList):
                    if(len(uni)==curSize):
                        flag = 0
                        if(curSize!=2):
                            y = list(combinations(uni,curSize-1))
                            #print y
                            flag = 0
                            for z in y:
                                #print tuple(sorted(list(z)))
                                if(tuple(sorted(tuple(z))) not in finalList):
                                    flag = 1
                                    break
                        if(flag == 0):
                            if(findCount(x,uni)>=supCount):
                                newPairs.append(uni)
                                #k+=1
                    #print "here"

                    #if(list(i.union(j)) not in finalList):
                                finalList.append(sort1) #CHANGE REPR------------
                                #print "Tuple :",
                                #print tuple(uni)
    
        curSize+=1
        #print k
        #k=0
        candidates = newPairs
    #print "Final list"
    #print finalList
    #retList = []
    #for i in finalList:
    #    retList.append([i,1])
    #return retList
    #print "Tupleee"
    #print tuple(finalList)
    return tuple(finalList)

def func1(x):
    return 1

def func2(x):
    return (x,1)

def countCheck(x,pairs):
    retList=[]
    x = list(x)
    tempSet = set()
    #print "----PAIRS----"
    #print type(pairs)
    #pairs=set(pairs)
    count =0
    for i in pairs:
        #print i[0]
        #print type(i[0])
        if(type(i[0])==unicode):
            temp = set()


            count+=1
            temp.add(i[0])
            #print "----------"
            #print temp
        else:
            temp = set(i[0])
            #print temp
        #print temp
        #map(literal_eval, i[0])
        retList.append([i[0],findCount(x,temp)])
    #print "fgfg"
    #print retList
    #print "COUNTTTTT"
    #print retList
    return retList


def main():
    
    start_time = time.time()

    
    inputFile = sys.argv[1]
    sup = float(sys.argv[2])
    output = sys.argv[3]
    rdd = sc.textFile(inputFile).map(lambda x:x.split(','))
    #print rdd.collect()
    size = len(rdd.collect())
    #sup = 0.3
    #print rdd
    rdd2 = rdd.mapPartitions(lambda x:apriori(x,sup)).map(lambda x:(x,1)).reduceByKey(lambda x,y:1)
    locaLFrequent1 =  rdd2.collect() #all frequent pairs

    #print rdd2.collect()
    supCount=sup*size
    #print supCount
    #locaLFrequent =[]
    #for i in locaLFrequent1:
    #    if(i[0] not in locaLFrequent):
    #        locaLFrequent.append(i[0])
    #print locaLFrequent
    #locaLFrequent = list(locaLFrequent)
    rdd3 = rdd.mapPartitions(lambda x: countCheck(x,locaLFrequent1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=supCount)
    #print "ANSWER----"
    answer = rdd3.collect()
    #print len(answer)
    #for i in answer:
        #print i[0],
        #print "  ",
        #print i[1]

    file = open(output,'w')
    for val in answer:
        #print type(val[0])
        if(type(val[0])==unicode):
            for i in range(len(val[0])):
                op = str(val[0][i])
                file.write("%s"%op)
            #file.write("\n")
        else:
            for i in range(len(val[0])):
                op = str(val[0][i])
                if(i!=(len(val[0])-1)):
                    file.write("%s,"%op)
                else:
                    file.write("%s"%op)
        file.write("\n")
    print("--- %s seconds ---" % (time.time() - start_time))

#rdd3 = rdd2.reduceByKey(lambda x,y:1)
#print rdd3.collect()

#rdd2 = rdd.mapPartitions(apriori)
#rdd3=rdd2.reduceByKey(func)
#rdd3.parrellelize





if __name__ == '__main__':
    main()
