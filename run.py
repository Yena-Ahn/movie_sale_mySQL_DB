import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import pymysql


# Problem 1 (5 pt.)
def data(table_name):
    data = pd.read_csv("data.csv")
    engine = create_engine('mysql+pymysql://DB2022_81863:DB2022_81863@astronaut.snu.ac.kr:7000/DB2022_81863', encoding='utf-8')
    if table_name == 'Director':
        director = pd.DataFrame(data['director']).rename(columns={'director': 'name'})
        director.drop_duplicates(inplace=True)
        director.to_sql(name='Director', con=engine, if_exists='append', index=False)
        return
    if table_name == 'Movie':
        movie = data[['title', 'price', 'director']]
        movie = movie.drop_duplicates(ignore_index=True)
        movie = movie.rename(columns={'director':'directorName'})
        movie.to_sql(name='Movie', con=engine, if_exists='append', index=False)
        return
    if table_name == 'Audience':
        audience = data[['name', 'gender', 'age']]
        audience = audience.drop_duplicates()
        audience.loc[audience['gender'] == "male", 'gender'] = 'M'
        audience.loc[audience['gender'] == "female", 'gender'] = 'F'
        audience.to_sql(name='Audience', con=engine, if_exists='append', index=False)
        return


def delete(cursor):
    # drop all tables in DB
    reset = ["SET @tables = NULL;",
             "SELECT GROUP_CONCAT(table_schema, '.', table_name) INTO @tables FROM information_schema.tables WHERE table_schema = 'DB2022_81863';",
             "SET @tables = CONCAT('DROP TABLE ', @tables);", "PREPARE stmt FROM @tables;", "EXECUTE stmt;",
             "DEALLOCATE PREPARE stmt;"]
    for statement in reset:
        cursor.execute(statement)

def reset(connection, cursor, status='Y'):


    if status == 'Y':
        delete(cursor)

    tables = {}

    # create table Director
    tables['Director'] = (
        "CREATE TABLE `Director` ("
        "    `name` varchar(20) NOT NULL,"
        "    PRIMARY KEY (`name`))"

    )

    # create table Movie
    tables['Movie'] = (
        "CREATE TABLE `Movie` ("
        "    `movieID` int NOT NULL AUTO_INCREMENT,"
        "    `title` varchar(55) NOT NULL,"
        "    `price` int,"
        "    `directorName` varchar(20) NOT NULL,"
        "    PRIMARY KEY (`movieID`),"
        "    FOREIGN KEY (`directorName`) REFERENCES `Director` (`name`))"
    )

    # create table Audience
    tables['Audience'] = (
        "CREATE TABLE `Audience` ("
        "    `audienceID` int NOT NULL AUTO_INCREMENT,"
        "    `name` varchar(10) NOT NULL,"
        "    `gender` char(1) NOT NULL,"
        "    `age` int NOT NULL,"
        "    CHECK ((`gender` = 'F') OR (`gender` = 'M')),"
        "    PRIMARY KEY (`audienceID`))"
    )

    # create table Booking
    tables['Booking'] = (
        "CREATE TABLE `Booking` ("
        "    `bookingID` int NOT NULL AUTO_INCREMENT,"
        "    `rating` int,"
        "    `movieID` int NOT NULL,"
        "    `audienceID` int NOT NULL,"
        "    PRIMARY KEY (`bookingID`),"
        "    FOREIGN KEY (`movieID`) REFERENCES `Movie` (`movieID`),"
        "    FOREIGN KEY (`audienceID`) REFERENCES `Audience` (`audienceID`),"
        "    CHECK ((`rating` <= 5) AND (`rating` >= 1)))"
    )

    print("Initializing databse...")
    for table in tables:
        try:
            print(f"Creating table '{table}':", end=" ")
            cursor.execute(tables[table])
            data(table)
        except mysql.connector.Error as e:
            if e.errno == mysql.connector.errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(e.msg)
        else:
            print('successfully created.')



    print('Initialized database')
    print()

# Problem 2 (3 pt.)
def print_movies(cursor):
    cursor.execute("SELECT m.movieID, m.title, m.directorName, m.price, count(b.bookingID), avg(b.rating)"
                   "FROM Movie m LEFT OUTER JOIN Booking b USING (movieID) "
                   "GROUP BY m.movieID, m.title, m.directorName, m.price"
                   )
    print("-" * (2+5+5+55+8+20+5+4+8+3+7+5))
    print("id" + " " * 5 + "title" + " " * 55 + "director" + " " * 20 + "price" + " " * 4 + "bookings" + " " * 3 + "ratings" + " " * 5)
    print("-" * (2+5+5+55+8+20+5+4+8+3+7+5))

    for (movieID, title, director, price, booking, rating) in cursor:
        print(str(movieID) + " " * (7-len(str(movieID))) + str(title) + " " * (60-len(str(title))) + str(director) + " " * (28-len(str(director))) + str(price) + " " * (9-len(str(price))) + str(booking) + " " * (11-len(str(booking))) + str(rating) + " " * (12-len(str(rating))))
    print("-" * (2+5+5+55+8+20+5+4+8+3+7+5))




# Problem 3 (3 pt.)
def print_audiences():
    # YOUR CODE GOES HERE

    
    # YOUR CODE GOES HERE
    pass

# Problem 4 (3 pt.)
def insert_movie():
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')
    

    # success message
    print('A movie is successfully inserted')
    # YOUR CODE GOES HERE
    pass

# Problem 6 (4 pt.)
def remove_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')


    # error message
    print(f'Movie {movie_id} does not exist')

    # success message
    print('A movie is successfully removed')
    # YOUR CODE GOES HERE
    pass

# Problem 5 (3 pt.)
def insert_audience():
    # YOUR CODE GOES HERE
    name = input('Audience name: ')
    gender = input('Audience gender: ')
    age = input('Audience age: ')
    

    # success message
    print('An audience is successfully inserted')
    # YOUR CODE GOES HERE
    pass

# Problem 7 (4 pt.)
def remove_audience():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')


    # error message
    print(f'Audience {audience_id} does not exist')

    # success message
    print('An audience is successfully removed')
    # YOUR CODE GOES HERE
    pass

# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')


    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'Audience {audience_id} does not exist')
    print('One audience cannot book the same movie twice')

    # success message
    print('Successfully booked a movie')
    # YOUR CODE GOES HERE
    pass

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')
    rating = input('Ratings (1~5): ')


    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'Audience {audience_id} does not exist')
    print(f'Wrong value for a rating')

    # success message
    print('Successfully rated a movie')
    # YOUR CODE GOES HERE
    pass

# Problem 10 (5 pt.)
def print_audiences_for_movie():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')

    
    # error message
    print(f'Audience {audience_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 11 (5 pt.)
def print_movies_for_audience():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')


    # error message
    print(f'Audience {audience_id} does not exist')
    # YOUR CODE GOES HERE
    pass


# Problem 12 (10 pt.)
def recommend():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')


    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'Audience {audience_id} does not exist')
    print('Rating does not exist')
    # YOUR CODE GOES HERE
    pass


# Total of 60 pt.
def main():
    # connect to mysql
    connection = mysql.connector.connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2022_81863',
        password='DB2022_81863',
        db='DB2022_81863',
        charset='utf8'
    )
    cursor = connection.cursor()

    # initialize database
    reset(connection, cursor, "Y")

    while True:
        print("WELCOME TO MOVIE BOOKING SYSTEM")
        print('============================================================')
        print('1. print all movies')
        print('2. print all audiences')
        print('3. insert a new movie')
        print('4. remove a movie')
        print('5. insert a new audience')
        print('6. remove an audience')
        print('7. book a movie')
        print('8. rate a movie')
        print('9. print all audiences who booked for a movie')
        print('10. print all movies rated by an audience')
        print('11. recommend a movie for an audience')
        print('12. exit')
        print('13. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            print_movies(cursor)
        elif menu == 2:
            print_audiences()
        elif menu == 3:
            insert_movie()
        elif menu == 4:
            remove_movie()
        elif menu == 5:
            insert_audience()
        elif menu == 6:
            remove_audience()
        elif menu == 7:
            book_movie()
        elif menu == 8:
            rate_movie()
        elif menu == 9:
            print_audiences_for_movie()
        elif menu == 10:
            print_movies_for_audience()
        elif menu == 11:
            recommend()
        elif menu == 12:
            print('Bye!')
            cursor.close()
            connection.close()
            break
        elif menu == 13:
            inputVal = input("Are you sure to reset the database? [Y/N] ")
            while inputVal != 'Y' and inputVal != 'N':
                print("Please choose between 'Y' or 'N'.")
                inputVal = input("Are you sure to reset the database? [Y/N] ")
            reset(connection, cursor, inputVal)
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()