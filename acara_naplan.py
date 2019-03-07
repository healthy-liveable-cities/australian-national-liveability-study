# import required libraries
from lxml import html
import requests
import time
import getpass
import random
import pandas
import numpy as np
from sqlalchemy import create_engine

# Define progress monitoring function
def progressor(num=0, denom=100, start=None, task=''):
    import time
    if (num < 0):
        print("Possible error: numerator is negative - is this right?")
    if num >= 0:
        pct = (float(num) / denom) * 100
        HMS = ''
        eta = ''
        mult = 9 + len(task)
        if type(start) in (int, float):
            secs = (time.time() - start)
            etaT = start + ((secs / (num + 0.001)) * denom)
            HMS = ' {} '.format(time.strftime("%H:%M:%S", time.gmtime(secs)))
            eta = ' (ETA: {}) '.format(time.strftime("%Y%m%d_%H%M", time.localtime(etaT)))
            mult += len(HMS) + len(eta)
        if num == 0:
            todayhour = time.strftime("%Y%m%d_%H%M")
            print("Start: {} ".format(todayhour))
            print("{:5.2f}% {}{}".format(0, task, ' ' * (mult - len(task) - 9))),
        print("\b" * mult),
        print("{:5.2f}%{}{} {}".format(pct, HMS, eta, task)),
        if num >= denom:
            todayhour = time.strftime("%Y%m%d_%H%M")
            print("\nComplete: {}".format(todayhour))
            if num > denom:
                print("\nPossible error: numerator is greater than denominator.  Is this right?")

# import excel file with ACARA data, indexed by school ACARA id
xls = pandas.ExcelFile('D:/ntnl_li_2018_template/data/destinations/acara_data_2017.xlsx')
df = pandas.read_excel(xls, index_col=3)

# make a list of tuples: data classes (Years 3, 5, 7, 9) and the table rows they are found on
years = [['year3',1],['year5',2],['year7',3],['year9',4]]
# make a list of tuples: traits and the table columns they are found on
traits = [['reading',2],['writing',3],['spelling',4],['grammar',5],['numeracy',6]]
# create blank entries for the trait values
# (as strings; we'll clean in post once we know what we're dealing with
# -- most are numbers, some are blank, others are '-';
# but there may be more options we're not aware of until we finish)
for year in years:
    for trait in traits:
        df["{}_{}".format(year[0],trait[0])] = ''
# enter in SQL connection details, used for output later
print("\nPlease enter PostgreSQL admin details...")
print('''(after processing the scraping, the data will be copied to SQL. 
A later update to this script should update SQL records for web scraped
records as they are processed.)''')
db_user = raw_input("Username: ")
db_pwd = getpass.getpass("Password for user {}: ".format(db_user))
db = 'li_australia_2018'
db_host = 'localhost'
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))
# define a grant query, so if required our non-admin scripts can access the data later
grant_query = '''
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO arc_sde;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arc_sde;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO python;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO python;
'''
# initialise a field for status (distinguish those with records, from those without)
df["naplan_status"]=''

# get list of acara IDs as a list (ie. those note previously processed)
# If for some reason the script crashes, you can restart processing interactively from this point
acara_schools = df[df["naplan_status"]==""].index.tolist()
## acara_schools = acara_schools[0:17]
# set the expected number of records to process to equal the length of the school list
completion_goal = len(acara_schools)
# initiate looping
i = 0
start = time.time()
for school in acara_schools:
    # get naplan web page for this school
    page = requests.get('https://www.myschool.edu.au/school/{}/naplan//numbers'.format(school))
    # parse content
    tree = html.fromstring(page.content)
    # check validity (if this field exists with content, it seems to be 'There is no data available')
    # so proceed if the following path has length zero (no content here suggests results are elsewhere)
    if len(tree.xpath('//*[@id="container"]/p[2]/text()'))==0:
        # loop over school years and traits
        for year in years:
            for trait in traits:
                # store field name into which to store results for this year-trait combination
                field = "{}_{}".format(year[0],trait[0])
                # get value for this year-trait combination
                text = tree.xpath('//*[@id="similarSchoolsTable"]/tbody/tr[{}]/td[{}]/text()'.format(year[1],trait[1]))
                # print("{},{}:{}".format(school,field,text))
                if len(text) != 0:
                    # if the length of this value is > 0, then remove white space and new line code to store the plain text
                    df.at[school,field] = text[0].strip()
                else:
                    # record the school field (presumably blank element)
                    df.at[school,field] =  ''.join(text)
        df.at[school,'naplan_status'] = 'Data recorded'
    else:
        # Record the no data status (something like "Data not recorded for this school")
        df.at[school,'naplan_status'] = tree.xpath('//*[@id="container"]/p[2]/text()')[0].strip()
    # increment counter
    i+=1
    # report on progress
    progressor(i,completion_goal,start,"{}/{} (ACARA ID #{})".format(i,completion_goal,school))
    # wait a small amount of time before proceeding
    time.sleep(5*random.random())

# convert string fields to integers with coercion of inappropriate values (ie "" and "-")
for year in years:
    for trait in traits:
        # store field name into which to store results for this year-trait combination
        field = "{}_{}".format(year[0], trait[0])
        df[field] = pandas.to_numeric(df[field], errors='coerce')

# copy dataframe to sql table using previously defined connection
df.to_sql(name='acara_schools',con=engine,if_exists='replace')

# The following sql queries were run interactively
additional_sql = '''
ANALYSE acara_schools;
CREATE TABLE acara_null_fraction AS
SELECT attname,
       null_frac
FROM pg_stats
WHERE pg_stats."tablename" = 'acara_schools';
SELECT * FROM acara_null_fraction;
SELECT naplan_status, COUNT(*) FROM acara_schools GROUP BY naplan_status;
SELECT "School_Sec", naplan_status,COUNT(*) FROM acara_schools GROUP BY "School_Sec",naplan_status ORDER BY "School_Sec",naplan_status;
CREATE TABLE acara_summary
(year int,
 trait text,
 count int,
 min    float,
 max    float,
 avg    float,
 stddev float);

INSERT INTO acara_summary SELECT 3 AS year, 'reading' AS trait, COUNT(year3_reading ), min(year3_reading ),max(year3_reading ) ,avg(year3_reading ), stddev(year3_reading )  FROM acara_schools;
INSERT INTO acara_summary SELECT 3 AS year, 'writing' AS trait, COUNT(year3_writing ), min(year3_writing ),max(year3_writing ) ,avg(year3_writing ), stddev(year3_writing )  FROM acara_schools;
INSERT INTO acara_summary SELECT 3 AS year, 'spelling' AS trait,COUNT(year3_spelling), min(year3_spelling),max(year3_spelling) ,avg(year3_spelling), stddev(year3_spelling)  FROM acara_schools;
INSERT INTO acara_summary SELECT 3 AS year, 'grammar' AS trait, COUNT(year3_grammar ), min(year3_grammar ),max(year3_grammar ) ,avg(year3_grammar ), stddev(year3_grammar )  FROM acara_schools;
INSERT INTO acara_summary SELECT 3 AS year, 'numeracy' AS trait,COUNT(year3_numeracy), min(year3_numeracy),max(year3_numeracy) ,avg(year3_numeracy), stddev(year3_numeracy)  FROM acara_schools;
INSERT INTO acara_summary SELECT 5 AS year, 'reading' AS trait, COUNT(year5_reading ), min(year5_reading ),max(year5_reading ) ,avg(year5_reading ), stddev(year5_reading )  FROM acara_schools;
INSERT INTO acara_summary SELECT 5 AS year, 'writing' AS trait, COUNT(year5_writing ), min(year5_writing ),max(year5_writing ) ,avg(year5_writing ), stddev(year5_writing )  FROM acara_schools;
INSERT INTO acara_summary SELECT 5 AS year, 'spelling' AS trait,COUNT(year5_spelling), min(year5_spelling),max(year5_spelling) ,avg(year5_spelling), stddev(year5_spelling)  FROM acara_schools;
INSERT INTO acara_summary SELECT 5 AS year, 'grammar' AS trait, COUNT(year5_grammar ), min(year5_grammar ),max(year5_grammar ) ,avg(year5_grammar ), stddev(year5_grammar )  FROM acara_schools;
INSERT INTO acara_summary SELECT 5 AS year, 'numeracy' AS trait,COUNT(year5_numeracy), min(year5_numeracy),max(year5_numeracy) ,avg(year5_numeracy), stddev(year5_numeracy)  FROM acara_schools;
INSERT INTO acara_summary SELECT 7 AS year, 'reading' AS trait, COUNT(year7_reading ), min(year7_reading ),max(year7_reading ) ,avg(year7_reading ), stddev(year7_reading )  FROM acara_schools;
INSERT INTO acara_summary SELECT 7 AS year, 'writing' AS trait, COUNT(year7_writing ), min(year7_writing ),max(year7_writing ) ,avg(year7_writing ), stddev(year7_writing )  FROM acara_schools;
INSERT INTO acara_summary SELECT 7 AS year, 'spelling' AS trait,COUNT(year7_spelling), min(year7_spelling),max(year7_spelling) ,avg(year7_spelling), stddev(year7_spelling)  FROM acara_schools;
INSERT INTO acara_summary SELECT 7 AS year, 'grammar' AS trait, COUNT(year7_grammar ), min(year7_grammar ),max(year7_grammar ) ,avg(year7_grammar ), stddev(year7_grammar )  FROM acara_schools;
INSERT INTO acara_summary SELECT 7 AS year, 'numeracy' AS trait,COUNT(year7_numeracy), min(year7_numeracy),max(year7_numeracy) ,avg(year7_numeracy), stddev(year7_numeracy)  FROM acara_schools;
INSERT INTO acara_summary SELECT 9 AS year, 'reading' AS trait, COUNT(year9_reading ), min(year9_reading ),max(year9_reading ) ,avg(year9_reading ), stddev(year9_reading )  FROM acara_schools;
INSERT INTO acara_summary SELECT 9 AS year, 'writing' AS trait, COUNT(year9_writing ), min(year9_writing ),max(year9_writing ) ,avg(year9_writing ), stddev(year9_writing )  FROM acara_schools;
INSERT INTO acara_summary SELECT 9 AS year, 'spelling' AS trait,COUNT(year9_spelling), min(year9_spelling),max(year9_spelling) ,avg(year9_spelling), stddev(year9_spelling)  FROM acara_schools;
INSERT INTO acara_summary SELECT 9 AS year, 'grammar' AS trait, COUNT(year9_grammar ), min(year9_grammar ),max(year9_grammar ) ,avg(year9_grammar ), stddev(year9_grammar )  FROM acara_schools;
INSERT INTO acara_summary SELECT 9 AS year, 'numeracy' AS trait,COUNT(year9_numeracy), min(year9_numeracy),max(year9_numeracy) ,avg(year9_numeracy), stddev(year9_numeracy)  FROM acara_schools;

SELECT year, trait, count, min, max, round(avg::numeric,2) AS avg, round(stddev::numeric,2) AS sd FROM acara_summary;
'''
