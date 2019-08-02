# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from decimal import Decimal
from cassandra.cluster import Cluster

### Creating list of filepaths to process original event csv data files ##
##########################################################################
def get_file_path_list():
    #get current folder and subfolder of event full_data_rows_list
    filepath = os.getcwd() + '/event_data'
    file_path_list = []
    # Create a for loop for creating a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root, '*'))

    retun(file_path_list)

### Processing the files to create the data file csv that will be used  ##
### for Apache Casssandra tables                                        ##
##########################################################################

def process_files(file_path_list):
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    for f in file_path_list:
        # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

            # extracting each data row one by one ans append it
            for line in csvreader:
                full_data_rows_list.append(line)

     # creating a smaller event data csv file called event_datafile_full csv \
     # that will be used to insert data into the Apache Cassandra tables
     csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

     with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
         writer = csv.writer(f, dialect='myDialect')
         writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
         for row in full_data_rows_list:
             if (row[0] == ''):
                 continue
             writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


     with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
         print(sum(1 for line in f))

     file = 'event_datafile_new.csv'
     retun(file)


### Query 1 Description:  In this query, `sessionId` column is used as the partition key and `itemInSession` as my clustering key. \ ##
## Each partition is uniquely identified by `sessionId` column while `itemInSession` column was used to uniquely identify the rows \ ##
## Within a partition to sort the data by the value of number.                                                                       ##
#######################################################################################################################################
def query1_executor(session, file):
    query1 = "CREATE TABLE IF NOT EXISTS heardSongs"
    query1 = query1 + "(sessionId int, itemInSession int, artist_name text, song_name text, length float, PRIMARY KEY(sessionId, itemInSession))"

    session.execute(query1)

    with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## Assigning the INSERT statements into the `query` variable
        query1 = "INSERT INTO heardSongs (sessionId, itemInSession, artist_name, song_name, length)"
        query1 = query1 + "VALUES (%s,%s,%s,%s,%s)"
        ## Assigning appropriete column element for each column in the INSERT statement.
        ## For e.g.,  INSERT artist_name and user first_name, corresponds to `line[0], line[1]` in the .csv file
        session.execute(query1, (int(line[8]), int(line[3]),line[0], line[9], float(line[5])))


def test_query1(session):
    ## This `SELECT` query returns the artist, song title and song's length in the music app history that was heard during \
    ## from `heardSongs` table where sessionId = 338, and itemInSession = 4

    query1 = "SELECT artist_name, song_name, length FROM heardSongs WHERE sessionId = 338 AND itemInSession = 4"
    rows = session.execute(query1)

    for row in rows:
        print (row.artist_name, row.song_name, row.length)


## Query 2 Description:  In this query, `userId` column and `sessionId` column together are used as the partition key and `itemInSession` as my clustering key. \       ##
## Each partition is uniquely identified by combination of `userId` column and `sessionId` column while `itemInSession` column was used to uniquely identify the rows \ ##
## Within a partition to sort the data by the value of number.                                                                                                          ##
##########################################################################################################################################################################
def query2_executor(session, file):
    query2 = "CREATE TABLE IF NOT EXISTS songsByUser"
    query2 = query2 + "(userId int, sessionId int, itemInSession int, artist_name text, song_name text, user_firstname text, user_lastname text, PRIMARY KEY((userId, sessionId), itemInSession))"

    session.execute(query2)

    with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## Assigning the INSERT statements into the `query` variables
        query2 = "INSERT INTO songsByUser (userId, sessionId, itemInSession, artist_name, song_name, user_firstname, user_lastname)"
        query2 = query2 + "VALUES (%s,%s,%s,%s,%s,%s,%s)"
        ## Assigning appropriete column element for each column in the INSERT statement.
        ## For e.g., INSERT userId and user sessionId, correspons to `line[10], line[8]` in .csv file
        session.execute(query2, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))


def test_query2(session):
    ## This `SELECT` query returns name of artist, song (sorted by itemInSession) and user (first and last name)\
    ## from `songsByUser` table for userid = 10, sessionid = 182

    query2 = "SELECT artist_name, song_name, user_firstname, user_lastname FROM songsByUser WHERE userId = 10 AND sessionId = 182 ORDER BY itemInSession ASC"
    rows = session.execute(query2)

    for row in rows:
        print (row.artist_name, row.song_name, row.user_firstname, row.user_lastname)


## Query 3 Description:  In this query, `song_name` column is used as the partition key and `userId` as my clustering key. \       ##
## Each partition is uniquely identified by combination of `song_name` column while `userId` column was used to uniquely identify the rows ##
## Within a partition to sort the data by the value of number.                                                                                                          ##
##########################################################################################################################################################################
def query3_executor(session, file):
    query3 = "CREATE TABLE IF NOT EXISTS usersBySong"
    query3 = query3 + "(song_name text,userId int, user_firstname text, user_lastname text, PRIMARY KEY(song_name, userId))"

    session.execute(query3)

    with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        ## Assigning the INSERT statements into the `query` variables
        query3 = "INSERT INTO usersBySong (song_name, userId, user_firstname, user_lastname)"
        query3 = query3 + "VALUES (%s,%s,%s,%s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT song_name and userId, you would change the code below to `line[9], line[10]`
        session.execute(query3, (line[9], int(line[10]), line[1],line[4]))


def test_query3(session):
    ## this `SELECT` query retuns every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

    query3 = "SELECT user_firstname, user_lastname FROM usersBySong WHERE song_name = 'All Hands Against His Own'"
    rows = session.execute(query3)

    for row in rows:
        print (row.user_firstname, row.user_lastname)

## Dropping tables and closing session and cluster ##
#####################################################
def drop_tables_and_session(session):
    query1 = "DROP TABLE heardSongs"
    query2 = "DROP TABLE songsByUser"
    query3 = "DROP TABLE usersBySong"
    session.execute(query1)
    session.execute(query2)
    session.execute(query3)


def main():
    # This should create a clusting and make a connection to a Cassandra instance on local machine
    # (127.0.0.1)
    cluster = Cluster()

    # To establish connection and begin executing queries, need a session
    session = cluster.connect()

    # Read and process files
    file_path_list = get_file_path_list()
    file = process_files(file_path_list)

    # Run and execute queries
    query1_executor(session, file)
    test_query1(session)

    query2_executor(session, file)
    test_query2(session)

    query3_executor(session, file)
    test_query3(session)

    drop_tables_and_session(session)

    ## Closing the session and cluster connection
    session.shutdown()
    cluster.shutdown()




if __name__ == '__main__':
    main()
