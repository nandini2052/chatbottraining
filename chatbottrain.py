import sqlite3
import pandas as pd

timeframes = ['2009-03']

for timeframe in timeframes:
    connection = sqlite3.connect('{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False

    while cur_length == limit:
    
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {}  and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        if df.count==0:
         
         last_unix = df.tail(1)['unix'].values[0]
         cur_length = len(df)

        if not test_done:
            with open('test.from1','a', encoding='utf8') as f:
                for content in df['parent'].values:
                    
                    f.write(str(content)+'\n')
                    

            with open('test.to1','a', encoding='utf8') as f:
                for content in df['comment'].values:
                    
                    f.write(str(content)+'\n')
                    

            test_done = True

        else:
            with open('train.from1','a', encoding='utf8') as f:
                for content in df['parent'].values:
                    
                    f.write(str(content)+'\n')

            with open('train.to1','a', encoding='utf8') as f:
                for content in df['comment'].values:
                    
                    f.write(str(content)+'\n')
       
        counter += 1
        
    

    
    
     
   
            
