from pyspark import SparkContext, SparkConf

def min_temperature():
    conf = SparkConf().setMaster("local").setAppName("MinTemperature")
    sc = SparkContext(conf=conf)

    weather_data = sc.textFile("file:////Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/weather_data.csv")
    min_weather_rdd = weather_data.map(parse).filter(lambda x: "TMIN" in x[1])
    min_temp_rdd = min_weather_rdd.map(lambda x: (x[0],x[2])).reduceByKey(lambda x,y: min(x,y))

    min_result = min_temp_rdd.collect()
    print "Station Id, Minimum Temperature"
    for res in min_result:
        print res[0], res[1]
    print "\n"

    max_weather_rdd = weather_data.map(parse).filter(lambda x: "TMAX" in x)
    max_temp_rdd = max_weather_rdd.map(lambda x: (x[0],x[2])).reduceByKey(lambda x,y: max(x,y))
    max_result = max_temp_rdd.collect()

    print "Station Id, Maximum Temperature"
    for res in max_result:
        print res[0], res[1]

def parse(weather_entry):
    entry = weather_entry.split(",")
    station_id, temp_type, temperature = entry[0], entry[2], entry[3]
    temperature = float(temperature)/10

    return (station_id, temp_type, temperature)

if __name__=="__main__":
    min_temperature()