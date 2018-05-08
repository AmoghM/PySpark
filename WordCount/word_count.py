from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setAppName("WordCount").setMaster("local")
sc = SparkContext(conf=conf)

def word_count():
    book_content = sc.textFile("file:////Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/Book.txt")
    book_rdd = book_content.flatMap(preprocess)
    word_value = book_rdd.map(lambda x: (x,1)) #initial freq is 1 for all words
    word_count_agg = word_value.reduceByKey(lambda x,y: x+y) #aggregating frequency of words
    sort_word_count_agg = word_count_agg.map(lambda (x,y):(y,x)).sortByKey() #sorting the words on the basis of freq. Freq has to be key.

    result = sort_word_count_agg.collect()
    print "WORD COUNT IN DECREASING ORDER"
    for res in result:
        word = res[1].encode("ascii","ignore")
        freq = str(res[0])
        print word +": "+ freq

def preprocess(line):
    process = re.compile(r'\W+', re.UNICODE).split(line.lower())
    return process

if __name__=="__main__":
    word_count()
