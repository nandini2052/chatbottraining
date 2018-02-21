import sqlite3
import pandas as pd




connection = sqlite3.connect('2007-02.db')
c = connection.cursor()
c.execute(' SELECT * FROM parent_reply WHERE unix > 0 and score > 0 ORDER BY unix ASC LIMIT 5000')
print(c.fetchone())
    
    
     
   
            
