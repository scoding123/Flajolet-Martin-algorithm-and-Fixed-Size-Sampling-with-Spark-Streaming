from pyspark import SparkContext
import sys
import math
import time
import binascii
import json

bin_array = []
param_a = 991
param_b = 383

output_string = ''
for i in range(0,30000):
    bin_array.append(0)
param_m = len(bin_array)

def hash_func(x):
    hash_list = []
    i = 0
    while i < 10:
        term_1 = i*x
        term_2 = i*param_a*param_b
        code = (term_1 + term_2)%param_m
        hash_list.append(code)
        i += 1
    
    return hash_list

def filter_fun_func(x,output_string,case=1):

    if case == 1:
        for c in x:
            if c != '' and c is not None:
                inp_hash = int(binascii.hexlify(c.encode('utf8')),16)
                hash_list = hash_func(inp_hash)
                for i in hash_list:
                    if bin_array[i] == 0:
                        bin_array[i] = 1

    if case == 2:
        for c in x:
            if c != '' and c is not None:
                inp_hash = int(binascii.hexlify(c.encode('utf8')),16)
                hash_list = hash_func(inp_hash)
                flag = 1

                for i in hash_list:
                    if bin_array[i] == 0:
                        flag = 0
                        break
                if flag == 0:
                    output_string += '0 '
                else:
                    output_string += '1 '

            else:
                output_string += '0 '
        return output_string


input_file1 = sys.argv[1]
input_file2 = sys.argv[2]
out_file = sys.argv[3]

sc = SparkContext()

start_time = time.time()
file1 = sc.textFile(input_file1).map(lambda x: json.loads(x)).map(lambda x: (x['city'])).collect()

filter_fun_func(file1,output_string,1)

file2 = sc.textFile(input_file2).map(lambda x: json.loads(x)).map(lambda x: (x['city'])).collect()

res = filter_fun_func(file2,output_string,2)

output_string.rstrip()
with open(out_file,'w') as f:
    f.write(res)

print('time taken',time.time() - start_time)