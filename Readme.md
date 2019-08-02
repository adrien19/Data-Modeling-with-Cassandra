# Data Modeling with Cassandra

This project deals with modeling data by creating tables in Apache Cassandra to run queries. It buils an ETL pipeline that transfers data from a set of CSV files within a directory (event_data) to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

Case example: A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.They'd like to create an Apache Cassandra database which can create queries on song play data to answer the questions.

## Installation

No installation required. Clone the project and run the etl.py file.

```bash
$python etl.py
```

Methods defined in etl.py:

```
def get_file_path_list():
    returns a list of paths to files.

def process_files():
    takes in "file_path_list" as arg, processes the data csv files and returns new csv file.

def query1_executor():
    takes in cassandra session and file as args and executes the query.

def test_query1():
    takes in session arg and executes query.
    prints query results.
    doesn't return anything.    

def query2_executor():
    takes in cassandra session and file as args and executes the query.
    doesn't return anything.


def test_query2():
    takes in session arg and executes query.
    prints query results.
    doesn't return anything.


def query3_executor():
    takes in cassandra session and file as args and executes the query.
    doesn't return anything.

def test_query3():
    takes in session arg and executes query.
    prints query results.
    doesn't return anything.


def main():
    declares and closes both Cassandra cluster and session. Calls the methods above.
```


## Usage

The raw data should be located in folder "event_data". Adjust the sql queries to meet the raw data sctructure being modeled.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## Authors

* **Adrien Ndikumana** - [adrien19](https://github.com/adrien19)


## Acknowledgments

* Inspiration from Udacity Team
