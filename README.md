# Movies_Database

In this program we will connect to a moviesdb that has been setup in postgresql. We will then create tables
and insert data into them that we obtained from text files. The text file data will be cleaned and put into
pandas dataframes before saving them as csv files. The csv files will then be copied to the database tables.
Queries will be done in the postgresql shell. This program ends with a function that prompts the user for
input to determine the best k movies in a certain span of years. That will be queried and then the results
will be saved as a csv file for the user.
