import pymysql
import numpy as np
from getpass import getpass
import argparse
from tqdm import tqdm

def remove_duplicates(password,db_name):
    """
    Removes duplicate annotations from the 'ets_annotations' table in the specified database. This function is part of a larger processing script that parses Zooniverse outputs into a MySQL database for analysis
    Parameters:
        password (str): MySQL password for connecting to the database.
        db_name (str): Name of the database containing the 'ets_annotations' table.
    Result:
        The cleaned data is saved in the 'ets_annotations_filtered' table.
    """
    print(f"Removing duplicates from ets_annotations in database {db_name}")
    db = pymysql.connect(host="localhost",user="root",password=password,db=db_name) #Open connection to database
    cursor=db.cursor() #Create cursor
    
    cursor.execute('select distinct(subject_id) from ets_annotations')
    subjects=np.concatenate(cursor.fetchall()) #Create array of distinct subject IDs that we will loop through to check for duplicate annotations
    cursor.execute('drop table if exists ets_annotations_filtered')
    cursor.execute('create table ets_annotations_filtered like ets_annotations')
    cursor.execute('insert into ets_annotations_filtered select * from ets_annotations') #Duplicate raw annotations table to operate on (preserving original ets_annotations for any future troubleshooting)
    
    for i in tqdm(subjects): #Loop through each subject
        cursor.execute(f"select case when count(distinct(user_name))=count(*) then 'true' else 'false' end as bool from ets_annotations_filtered where subject_id={i}")  #Determine if, for given subject_id, the number of annotations = number of users who annotated. If False, implies multiple annotations from same user
        if cursor.fetchall()[0][0]=='true': #Case where no duplicate annotations so continue to next subject_id
            continue
        
        cursor.execute(f"select user_name,MIN(id) from ets_annotations_filtered where subject_id={i} group by user_name having count(*)>1;") #Finds user names and the minimum ID number assigned to their annotations of the given subject (each annotation is numbered in time order)
        fetch=cursor.fetchall()
        for j in fetch: #Loops through for each user if there are multiple users who submitted multiple annotations
            dup_user=j[0] #Creates variable with user_name of the given user
            min_id=str(j[1]) #Creates variable of the minimum ID number assigned to their annotations of the given subject
            cursor.execute(f"delete from ets_annotations_filtered where ((subject_id={i}) and (user_name='{dup_user}') and (id>'{min_id}'))") #Deletes all annotations by the given user where the annotation ID number of greater than the minimum ID value, i.e. all classifcations submitted after the user's initial classification of the subject
            
    db.commit()            
    cursor.close()
    db.close()
    print(f"Finished removing duplicates, result in ets_annotations_filtered table in database {db_name}")
    
if __name__ == '__main__':
    password=getpass("MySQL password: ") #Password entry for connecting to local MySQL server
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-D','--db_name',required=True,type=str,help='Name of database') #Database name entered in command line is parsed using argparse package
    args = parser.parse_args()
    
    remove_duplicates(password,args.db_name)