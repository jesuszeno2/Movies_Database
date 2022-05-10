"""
Jesus Zeno
In this program we will connect to a moviesdb that has been setup in postgresql. We will then create tables
and insert data into them that we obtained from text files. The text file data will be cleaned and put into
pandas dataframes before saving them as csv files. The csv files will then be copied to the database tables.
Queries will be done in the postgresql shell. This program ends with a function that prompts the user for
input to determine the best k movies in a certain span of years. That will be queried and then the results
will be saved as a csv file for the user.
"""

import psycopg2
import pandas as pd
import numpy as np

# Connect to the test db and create a cursor
conn = psycopg2.connect("host=localhost dbname=moviesdb user=postgres password=anything")
curr = conn.cursor()

# Execute a create table statement in the database
curr.execute("""
        CREATE TABLE Movie(
          id INT NOT NULL,
          name VARCHAR(256),
          year INT,
          rank FLOAT,
          PRIMARY KEY (id)
        );

        CREATE TABLE Person(
          id INT NOT NULL,
          fname VARCHAR(128),
          lname VARCHAR(128),
          gender VARCHAR(128),
          PRIMARY KEY (id)
        );

        CREATE TABLE Director(
          id INT NOT NULL,
          fname VARCHAR(128),
          lname VARCHAR(128),
          PRIMARY KEY (id)
        );

        CREATE TABLE ActsIn(
          pid INT NOT NULL,
          mid INT NOT NULL,
          role VARCHAR(128),
          FOREIGN KEY (mid) REFERENCES Movie(id),
          FOREIGN KEY (pid) REFERENCES Person(id)
        );

        CREATE TABLE temp_directs(
          did INT NOT NULL,
          mid INT NOT NULL
        );

        CREATE TABLE Directs(
          did INT NOT NULL,
          mid INT NOT NULL,
          FOREIGN KEY (mid) REFERENCES Movie(id),
          FOREIGN KEY (did) REFERENCES Director(id)
        );
""")

conn.commit()

# Define empty lists to be populated later in the respective dataframes
movies_first_split = []

# Open text file with movies and go through each line to split properly.
with open('IMDBMovie.txt', 'r', encoding='latin-1') as movie_file:
    next(movie_file)  # skips header row
    # Iterate through text file and append to list
    for line in movie_file:
        line = line.strip()
        first_split = line.split("),")  # Use as unique initial splitter due to format. Get data in two halves
        movies_first_split.append(first_split)

# Clean up the dataframe. Rename columns first.
initial_movies_df = pd.DataFrame(movies_first_split, columns=["first half", "second half"])
# Split the two halves into appropriate columns with names. Ensure it's split into exactly two with n=1.
initial_movies_df[['id', 'name']] = initial_movies_df['first half'].str.split(',', n=1, expand=True)
initial_movies_df[['year', 'rank']] = initial_movies_df['second half'].str.split(',', n=1, expand=True)
# Drop unnecessary columns
initial_movies_df = initial_movies_df.drop(['first half', 'second half'], axis=1, index=None)
# Tidy up the name column to put the ')' back in.
initial_movies_df['name'] = initial_movies_df['name'].astype(str) + ')'
# Convert empty strings in rankings to NaN and then replace with -1
# -1 was chosen in case a movie actually scored a 0
initial_movies_df['rank'] = initial_movies_df['rank'].replace(r'^\s*$', np.NaN, regex=True)
initial_movies_df['rank'] = initial_movies_df['rank'].fillna(-1)
# Assign data type to match SQL data types
initial_movies_df = initial_movies_df.astype({'id': 'int', 'name': 'str', 'year': 'int', 'rank': 'float'})
# Apply unique delimiter
initial_movies_df.to_csv('Movies_Table.csv', index=False, sep='|')

# Read text file with people and put directly into dataframe. Update the id data type.
person_df = pd.read_csv(r'IMDBPerson.txt', encoding='latin-1')
person_df = person_df.astype({'id': 'int'})
# print(person_df.dtypes)
person_df.to_csv('Person_Table.csv', index=None)

# Read text file with directors and put directly into dataframe. Update the id data type.
directors_df = pd.read_csv(r'IMDBDirectors.txt', encoding='latin-1')
directors_df = directors_df.astype({'id': 'int'})
directors_df.to_csv('Directors_Table.csv', index=None)

# create empty cast list
cast_initial_split = []
# Open text file with movies and go through each line to split properly.
with open('IMDBCast.txt', 'r', encoding='latin-1') as cast_file:
    next(cast_file)  # skips header row
    # Iterate through text file and append to list
    for line in cast_file:
        line = line.strip()
        # Use as unique initial splitter due to format. Stop separating after the 2nd comma.
        cast_split = line.split(",", 2)
        cast_initial_split.append(cast_split)

# Clean up the dataframe. Rename columns first. Update column types
cast_df = pd.DataFrame(cast_initial_split, columns=["pid", "mid", "role"])
# Strip exit characters in role column
cast_df['role'] = cast_df['role'].str.strip('\\')
# Rename columns and save as csv with unique delimiter.
cast_df = cast_df.astype({'pid': 'int', 'mid': 'int'})
cast_df.to_csv('Cast_Relationship_Table.csv', index=None, sep='|')

# Read text file with Movie Directors and put directly into dataframe. No need to split this up like movies.
movie_directors_df = pd.read_csv(r'IMDBMovie_Directors.txt', encoding='latin-1')
# Update column types
movie_directors_df = movie_directors_df.astype({'did': 'int', 'mid': 'int'})
movie_directors_df.to_csv('Movie_Directors_Relationship_Table.csv', index=None)

# Open the Movies_Table.csv file to copy the contents into the 'movie' table in the SQL server
with open('Movies_Table.csv', 'r', encoding='latin-1') as movie_table_insert:
    next(movie_table_insert)  # skips header row
    curr.copy_from(movie_table_insert, 'movie', null='', sep='|')
    conn.commit()
    movie_table_insert.close()

# Open the Person_Table.csv file to copy the contents into the 'person' table in the SQL server
with open('Person_Table.csv', 'r', encoding='latin-1') as person_table_insert:
    next(person_table_insert)  # skips header row
    curr.copy_from(person_table_insert, 'person', sep=',')
    conn.commit()
    person_table_insert.close()

# Open the Directors_Table.csv file to copy the contents into the 'director' table in the SQL server
with open('Directors_Table.csv', 'r', encoding='latin-1') as director_table_insert:
    next(director_table_insert)  # skips header row
    curr.copy_from(director_table_insert, 'director', sep=',')
    conn.commit()
    director_table_insert.close()

# Open the Cast_Relationship_Table.csv file to copy the contents into the 'ActsIn' table in the SQL server
with open('Cast_Relationship_Table.csv', 'r', encoding='latin-1') as cast_table_insert:
    next(cast_table_insert)  # skips header row
    curr.copy_from(cast_table_insert, 'actsin', sep='|')  # Unique delimiter needed
    conn.commit()
    cast_table_insert.close()

# Open the Movie_Directors_Relationship_Table.csv file to copy the contents into the 'temp_directs'
# table in the SQL server. This will allow us to bypass issues with FK constraints.
with open('Movie_Directors_Relationship_Table.csv', 'r', encoding='latin-1') as \
        movie_directors_relationship_table_insert:
    next(movie_directors_relationship_table_insert)  # skips header row
    curr.copy_from(movie_directors_relationship_table_insert, 'temp_directs', sep=',')
    conn.commit()
    movie_directors_relationship_table_insert.close()

# Copy only the rows that meet the FK constraints into the actual 'Directs' SQL table
curr.execute("""
    INSERT INTO Directs
    SELECT *
        FROM temp_directs
    WHERE EXISTS (SELECT 1 FROM Movie, Director 
                    WHERE Movie.id = temp_directs.mid
                    AND Director.id = temp_directs.did);

    DROP TABLE temp_directs;

    """)

conn.commit()


def find_best_movies_in_years(best_number, start_year, end_year):
    # SQL code for finding the best movie in a given set of years
    curr.execute("""
    SELECT name 
    FROM movie 
    WHERE year >= {start_year} AND year <= {end_year} ORDER BY rank DESC LIMIT {best_number};
    """.format(start_year=start_year, end_year=end_year, best_number=best_number))
    # Put results into a python list
    best_movie_query_result = curr.fetchall()

    return best_movie_query_result


# Prompt user for inputs to use in find best movies in years function
best_number = int(input("How many best movies are you looking for? "))
start_year = int(input("What year do you want to start looking from? "))
end_year = int(input("What year do you want to end at? "))

# Call the function for best movies in given years and pass the user input as the parameters.
# Rename column to best movies and send the df to csv with ';' as delimiter.
best_movies_df = pd.DataFrame(find_best_movies_in_years(best_number, start_year, end_year),
                              columns=['Best Movies'])
best_movies_df.to_csv('best_n_movies_from_years_x_to_y.csv', index=None, sep=';')

# Don't forget to close the connection
conn.close()
