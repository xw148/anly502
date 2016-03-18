#!/usr/bin/spark-submit
#
# Problem Set #4
# Implement wordcount on the shakespeare plays as a spark program that:
# a.Removes characters that are not letters, numbers or spaces from each input line.
# b.Converts the text to lowercase.
# c.Splits the text into words.
# d.Reports the 40 most common words, with the most common first.

# Note:
# You'll have better luck debugging this with ipyspark

import sys
import matplotlib.pyplot as plt
from operator import add
from pyspark import SparkContext
import datetime

if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps03/freebase-wex-2009-01-12-articles.tsv'

    ## 
    ## Run WordCount on Spark
    ##

    sc = SparkContext( appName="Wikipedia Count" )
    lines = sc.textFile( infile )
    counts = lines.map(lambda line: line.split("\t")[2])\
    .map(lambda word: (word[0:7],1))\
    .reduceByKey(add)

    sortdate = counts.sortBy(lambda x: x[0]).collect()


    with open("wikipedia_by_month.txt","w") as fout:
        for (date, count) in sortdate:
            fout.write("{}\t{}\n".format(date,count))

    x = []
    y = []    
    labels = []
    i = 1
    for (date,count) in sortdate:
	   x.append(i)
	   y.append(count)
	   i = i+1
           labels.append(date)

    plt.plot(x, y)
    fig =  plt.figure()
    plt.xticks(x,labels,rotation='vertical')
    fig.autofmt_xdate()
    plt.show()
    plt.savefig("wikipedia_by_month.pdf")
    
    ## 
    ## Terminate the Spark job
    ##

    sc.stop()
