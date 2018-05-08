from pyspark import SparkContext,SparkConf

def find_most_popular():
    conf = SparkConf().setMaster("local").setAppName("MostPopular")
    sc = SparkContext(conf=conf)
    movie_data = sc.textFile('file:////Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/ml-100k/u.data')
    # movie data format: user id, movie id, rating, timestamp
    movie_rdd = movie_data.map(lambda x: (int(x.split()[1]),1))
    popular_movie = movie_rdd.reduceByKey(lambda x,y: x+y).map(lambda (x,y):(y,x)).sortByKey()

    result = popular_movie.collect()
    print "Most popular movie in ascending order:"
    for res in result:
        print("movie id: %s , count: %s" %(res[1],res[0]))


if __name__=='__main__':
    find_most_popular()

