import  pandas as pd
import json
import  numpy as np
d=[]
with open('test1file.txt','r') as fs:
    for line in fs:
        l=json.loads(line)
        data=pd.read_json(l['content'])
        title = data[:1]
        data = data[1:]
        for i in range(data.shape[1]):
            label='axis{}'.format(i)
            data[label]=title[i].values[0]

print (data)