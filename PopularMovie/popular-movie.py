from pyspark import SparkContext,SparkConf

def find_most_popular():
    conf = SparkConf().setMaster("local").setAppName("MostPopular")
    sc = SparkContext(conf=conf)
    movie_name = sc.broadcast(movie_name_id_mapper())

    movie_data = sc.textFile('file:////Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/ml-100k/u.data')
    # movie data format: user id, movie id, rating, timestamp
    movie_rdd = movie_data.map(lambda x: (int(x.split()[1]),1))
    popular_movie = movie_rdd.reduceByKey(lambda x,y: x+y).map(lambda (x,y):(y,x)).sortByKey()
    movie_name_rdd = popular_movie.map(lambda (freq, id) : (movie_name.value[id],freq))

    result = movie_name_rdd.collect()
    print "Most popular movie in ascending order:"
    for res in result:
        print("movie %s , count: %s" %(res[0],res[1]))

def movie_name_id_mapper():
    movie_name = {}
    with open("/Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/ml-100k/u.ITEM") as f:
        for itr in f:
            field = itr.split('|')
            movie_name[int(field[0])] = field[1]
    return movie_name

if __name__=='__main__':
    find_most_popular()

