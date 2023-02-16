#Matt Bennett
#CNE 340 2/8/2023
#Create a program to import new jobs into a table by utilizing an API

import mysql.connector
import time
import json
import requests
from datetime import datetime
import html2text


# Connect to database
# You may need to edit the connect function based on your local settings.
# I made a password for my database because it is important to do so. Also make sure MySQL server is running or it will not connect
def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1', database='cne340')
    return conn


# Create the table structure
def create_tables(cursor):
    # Creates table
    # Must set Title to CHARSET utf8 unicode Source: http://mysql.rjweb.org/doc.php/charcoll.
    # Python is in latin-1 and error (Incorrect string value: '\xE2\x80\xAFAbi...') will occur if Description is not in unicode format due to the json data
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (id INT PRIMARY KEY auto_increment, Job_ID varchar(50),
    Company varchar(300), Created_at DATE, Url varchar(1000), Title varchar(500), Salary varchar(500), Tags varchar(3000),
    Description LONGBLOB); ''') #Updated to reflect more of the information received from the API, only used what seemed relevant


# Query the database.
# You should not need to edit anything in this function
def query_sql(cursor, query):
    cursor.execute(query)
    return cursor


# Add a new job
def add_new_job(cursor, jobdetails):
    job_id = jobdetails['id']
    company = jobdetails['company_name']
    url = jobdetails['url']
    title = jobdetails['title']
    salary = jobdetails['salary']
    tags = ""
    for tag in jobdetails['tags']:    #Had to de-list-ify the tags column because mysql wouldnt accept a list as an input
        tags = tags + tag + ", "
    description = html2text.html2text(jobdetails['description'])
    date = jobdetails['publication_date'][0:10]
    query = cursor.execute("INSERT INTO jobs(Description, Created_at, Job_ID, Company, Url, Title, Salary, Tags)"  #Added Job_ID, Company, Url, Title, Salary, Tags
               "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", (description, date, job_id, company, url, title, salary, tags))
     # %s is what is needed for Mysqlconnector as SQLite3 uses ? the Mysqlconnector uses %s
    return query_sql(cursor, query)


# Check if new job
def check_if_job_exists(cursor, jobdetails):
    id = jobdetails['id']
    query = "SELECT * FROM jobs WHERE Job_ID = \"%s\"" % id
    return query_sql(cursor, query)

# Deletes job
def delete_job(cursor, jobdetails):
    id = jobdetails['id']
    query = "DELETE FROM jobs WHERE Job_ID = \"%s\"" % id
    return query_sql(cursor, query)


# Grab new jobs from a website, Parses JSON code and inserts the data into a list of dictionaries do not need to edit
def fetch_new_jobs():
    query = requests.get("https://remotive.io/api/remote-jobs")
    datas = json.loads(query.text)
    return datas


# Main area of the code. Should not need to edit
def jobhunt(cursor):
    # Fetch jobs from website
    jobpage = fetch_new_jobs()  # Gets API website and holds the json data in it as a list
    # use below print statement to view list in json format
    # print(jobpage)
    add_or_delete_job(jobpage, cursor)

# Gives the delta of two dates, taken from https://stackoverflow.com/questions/8419564/difference-between-two-dates-in-python
def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return (abs((d2 - d1).days))

def add_or_delete_job(jobpage, cursor):
    # Add your code here to parse the job page
    for jobdetails in jobpage['jobs']:  # EXTRACTS EACH JOB FROM THE JOB LIST. It errored out until I specified jobs. This is because it needs to look at the jobs dictionary from the API. https://careerkarma.com/blog/python-typeerror-int-object-is-not-iterable/
        check_if_job_exists(cursor, jobdetails)
        is_job_found = len(cursor.fetchall()) > 0  # https://stackoverflow.com/questions/2511679/python-number-of-rows-affected-by-cursor-executeselect
        if is_job_found: #Avoiding duplication of postings
            if days_between(datetime.today().strftime('%Y-%m-%d'), jobdetails['publication_date'][0:10]) > 14: #if older than 14 days, delete
                delete_job(cursor, jobdetails)
                print("Deleting a job! BAM!") #Added so I had visual indication of deletion
            else:
                pass
        elif days_between(datetime.today().strftime('%Y-%m-%d'), jobdetails['publication_date'][0:10]) <= 14: #if newer than (or equal to) 14 days, add
            add_new_job(cursor, jobdetails)
            print(f"There's a new job for you to check out! It's a {jobdetails['title']} position!") #Let the people know
        else:
            pass

# Setup portion of the program. Take arguments and set up the script
# You should not need to edit anything here.
def main():
    # Important, rest are supporting functions
    # Connect to SQL and get cursor
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor)

    while (1):  # Infinite Loops. Only way to kill it is to crash or manually crash it. We did this as a background process/passive scraper
        print(f"Doing a check! {datetime.today()}") #Added for visual representations of progress
        jobhunt(cursor)
        print("Check complete, back in 4 hrs") #Added for visual representations of progress
        time.sleep(14400)  # Sleep for 1h, this is ran every hour because API or web interfaces have request limits. Your request will get blocked.
                            #Changed to every 4 hrs per the rubric

# Sleep does a rough cycle count, system is not entirely accurate
# If you want to test if script works change time.sleep() to 10 seconds and delete your table in MySQL
if __name__ == '__main__':
    main()

