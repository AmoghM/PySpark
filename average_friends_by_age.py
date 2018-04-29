from pyspark import SparkConf, SparkContext


def find_friends_by_age():
    conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
    sc = SparkContext(conf=conf)

    #row_id, name, age, total friends
    social_media_dataset = sc.textFile(
        "file:////Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/social_media_dataset.csv")
    social_media_rdd = social_media_dataset.map(read_data) #parse each line of the dataset

    key_value_rdd = social_media_rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) # count of people and sum of total friends
    avg_friends = key_value_rdd.mapValues(lambda x: x[0] / x[1]) #average number of friends.
    age_avg_friends = avg_friends.collect()

    for res_line in age_avg_friends:
        print res_line


def read_data(line):
    line_split = line.split(",")
    age = int(line_split[2])
    friends = int(line_split[3])
    return (age, friends)


if __name__ == "__main__":
    find_friends_by_age()
