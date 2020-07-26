from pyspark import SparkContext
import sys
import math
import time
import binascii
import json
from pyspark.streaming import StreamingContext
import datetime
import random

max_zero = -1*sys.maxsize
seed = 5
random.seed(seed)

prime_set_1 = [11, 13, 19, 23, 31, 43, 67, 73]
prime_set_2 = [183, 249, 423, 447, 767, 873, 897, 919]
prime_set_3 = [2183, 4593, 7937, 6317, 6107, 6107 ,3137, 23571]
m = 16**2

hash_vals = []

for i in range(8):
    val1 = random.choice(prime_set_1)
    prime_set_1.remove(val1)
    val2 = random.choice(prime_set_2)
    prime_set_2.remove(val2)
    val3 = random.choice(prime_set_3)
    prime_set_3.remove(val3)
    hash_vals.append([val1,val2,val3])


port_num = int(sys.argv[1])
output_file = sys.argv[2]

sc = SparkContext()
ssc = StreamingContext(sc, 5)


start_time = time.time()

with open(output_file, "w") as f:
    f.write("Time,Ground Truth,Estimation\n")

def hash_fun(x):

    real_num_cities = len(set(x))
    time_stamp = datetime.datetime.now()
    upd_format = time_stamp.strftime("%Y-%m-%d %H:%M:%S")
    res_cities = []

    for i in range(len(hash_vals)):
        res_cities.append(0)

    for i in x:
        if i != '' and i is not None:
            hash_val = int(binascii.hexlify(i.encode('utf8')),16)

            j = 0
            while j < len(hash_vals):
                a = hash_val*hash_vals[j][0]
                b = hash_vals[j][1]
                p = hash_vals[j][2]
                value = ((a+b)%p)%m
                bin_value = bin(value)[2:] #binary without leading 0b
                init_len = len(bin_value)
                len_without_zero = len(bin_value.rstrip('0'))
                num_of_zero = init_len - len_without_zero

                if num_of_zero > res_cities[j]:
                    res_cities[j] = num_of_zero

                j += 1
    res = [2**i for i in res_cities]
    new_arr = []

    for i in range(0,len(res),2):
        new_arr.append((res[i]+res[i+1])/float(2))

    new_arr.sort()

    estimation = (new_arr[1]+new_arr[2])/float(2)

    with open(output_file,'a') as f:
        f.write(str(upd_format) + ',' + str(real_num_cities) + ',' + str(estimation) + '\n')


input_rdd = ssc.socketTextStream("localhost",port_num)
input_rdd = input_rdd.map(json.loads).map(lambda x: (x['city'])).window(30,10).foreachRDD(lambda x: hash_fun(x.collect()))

ssc.start()
ssc.awaitTermination()
print('time taken',time.time() - start_time)