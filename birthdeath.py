import pandas as pd
import requests
import csv

url_1 = "https://www.dropbox.com/s/kvv9tl40wsujist/all_heifers.csv?dl=0"
url = url_1.replace('dl=0','dl=1')
# 
response = requests.get(url)
# 
if response.status_code == 200:
    content = response.content
    local_file_path = 'F:\\COWS\\data\\testdata\\all_preg.csv'
    with open ('F:\\COWS\\data\\testdata\\all_preg.csv', 'wb') as file:
        file.write(content)
    
    print('success')
else:
    print('fukkup')
    
print('')
        
        
        
# ddate   = bd1['death_date']
# bdate   = bd1['birth_date']
# WY_id   = bd1['WY_id']
# print(bd1)
