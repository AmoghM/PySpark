from pyspark import SparkContext, SparkConf

def customer_amount():
    conf = SparkConf().setMaster("local").setAppName("Ecommerece")
    sc = SparkContext(conf=conf)

    #Customer ID, Item ID, Amount Spent
    ecomm_dataset = sc.textFile("file:////Users/amoghmishra/Desktop/AmoghM/ApacheSpark/dataset/customer-orders.csv")
    ecomm_rdd = ecomm_dataset.map(preprocess)
    cust_amt = ecomm_rdd.reduceByKey(lambda x,y : x+y).map(lambda (x,y): (y,x)) # (cust_id,amt) --> (amt,cust_id)
    sort_cust_amt = cust_amt.sortByKey() #sorted in ascending order on the basis of amount

    result = sort_cust_amt.collect()
    print "TOTAL AMOUNT SPENT BY THE CUSTOMER IN ASCENDING ORDER"
    for res in result:
        print res[1], res[0]


def preprocess(line):
    data = line.split(",")
    cust_id = int(data[0])
    amt = float(data[2])
    return (cust_id,amt)

if __name__=="__main__":
    customer_amount()
