import math

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

def create_table(cursor):
    tables = {}

    # create table Director
    tables['Director'] = (
        "CREATE TABLE `Director` ("
        "    `name` varchar(50) NOT NULL,"
        "    PRIMARY KEY (`name`))"

    )

    # create table Movie
    tables['Movie'] = (
        "CREATE TABLE `Movie` ("
        "    `movieID` int NOT NULL AUTO_INCREMENT,"
        "    `title` varchar(100) NOT NULL,"
        "    `price` int,"
        "    `directorName` varchar(50) NOT NULL,"
        "    PRIMARY KEY (`movieID`),"
        "    FOREIGN KEY (`directorName`) REFERENCES `Director` (`name`))"
    )

    # create table Audience
    tables['Audience'] = (
        "CREATE TABLE `Audience` ("
        "    `audienceID` int NOT NULL AUTO_INCREMENT,"
        "    `name` varchar(50) NOT NULL,"
        "    `gender` char(1) NOT NULL,"
        "    `age` int NOT NULL,"
        "    CHECK ((`gender` = 'F') OR (`gender` = 'M')),"
        "    PRIMARY KEY (`audienceID`))"
    )

    # create table Booking
    tables['Booking'] = (
        "CREATE TABLE `Booking` ("
        "    `bookingID` int NOT NULL AUTO_INCREMENT,"
        "    `movieID` int NOT NULL,"
        "    `audienceID` int NOT NULL,"
        "    `rating` int,"
        "    PRIMARY KEY (`bookingID`),"
        "    FOREIGN KEY (`movieID`) REFERENCES `Movie` (`movieID`),"
        "    FOREIGN KEY (`audienceID`) REFERENCES `Audience` (`audienceID`),"
        "    CHECK ((`rating` <= 5) AND (`rating` >= 1)))"

    )

    # # create table Rating
    # tables['Rating'] = (
    #     "CREATE TABLE `Rating` ("
    #     "    `bookingID` int NOT NULL,"
    #     "    `rating` int,"
    #     "    PRIMARY KEY (`bookingID`),"
    #     "    FOREIGN KEY (`bookingID`) REFERENCES `Booking` (`bookingID`),"
    #     "    CHECK ((`rating` <= 5) AND (`rating` >= 1)))"
    # )
    for table in tables:
        cursor.execute(tables[table])



def delete(cursor):
    # drop all tables in DB
    reset = ["SET @tables = NULL;",
             "SELECT GROUP_CONCAT(table_schema, '.', table_name) INTO @tables FROM information_schema.tables WHERE table_schema = 'DB2022_81863';",
             "SET @tables = CONCAT('DROP TABLE ', @tables);", "PREPARE stmt FROM @tables;", "EXECUTE stmt;",
             "DEALLOCATE PREPARE stmt;"]
    for statement in reset:
        cursor.execute(statement)



def reset(cursor, status="Y"):
    tables = ["Director", "Movie", "Audience", "Booking"]
    if status == 'Y':
        print("Initializing database...")
        delete(cursor)
        create_table(cursor)
        for table in tables:
            try:
                print(f"Creating table '{table}':", end=" ")
                data(table)
            except mysql.connector.Error as e:
                    print(e.msg)
            else:
                print('successfully created.')
        print('Initialized database')
        print()
    else:
        print("User has not reset database. Database is not cleared.")
        print()

# Problem 2 (3 pt.)
def print_movies(cursor):
    cursor.execute("SELECT m.movieID, m.title, m.directorName, m.price, count(b.bookingID), avg(b.rating)"
                   "FROM Movie m LEFT OUTER JOIN Booking b USING (movieID) "
                   "GROUP BY m.movieID, m.title, m.directorName, m.price"
                   )
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
    print("id" + " " * 5 + "title" + " " * 100 + "director" + " " * 50 + "price" + " " * 4 + "bookings" + " " * 3 + "ratings" + " " * 5)
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))

    for (movieID, title, director, price, booking, rating) in cursor:
        print(str(movieID) + " " * (7-len(str(movieID))) + str(title) + " " * (105-len(str(title))) + str(director) + " " * (58-len(str(director))) + str(price) + " " * (9-len(str(price))) + str(booking) + " " * (11-len(str(booking))) + str(rating) + " " * (12-len(str(rating))))
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))




# Problem 3 (3 pt.)
def print_audiences(cursor):
    cursor.execute("SELECT *"
                   "FROM Audience"
                   )
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
    print("id" + " " * 5 + "name" + " " * 100 + "gender" + " " * 5 + "age" + " " * 5)
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
    for (id, name, gender, age) in cursor:
        print(str(id) + " " * (7-len(str(id))) + name + " " * (104-len(name)) + gender + " " * 10 + str(age) + " " * (8-len(str(age))))
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))

# Problem 4 (3 pt.)
def insert_movie(cursor):
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')


    if not price.isdigit() or director.isdigit():
        print("[ERROR] Please input correct data type.")
        return

    try:
        cursor.execute(f"INSERT INTO Director VALUES ('{director}')")
        cursor.execute(f"INSERT INTO Movie VALUES (NULL, '{title}', {price}, '{director}')")
        print('A movie is successfully inserted.')
        print()
    except mysql.connector.Error as e:
        print("[ERROR] " + e.msg)
        return




# Problem 6 (4 pt.)
def remove_movie(cursor):
    movie_id = input('Movie ID: ')

    if not movie_id.isdigit():
        print("[ERROR] Please insert correct integer value.")
        return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Movie WHERE movieID = {movie_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Movie {movie_id} does not exist. Please insert correct Movie ID.')
            return

        if bool == 1:
            cursor.execute(f"DELETE FROM Booking WHERE movieID = {movie_id}")
            cursor.execute(f"DELETE FROM Movie WHERE movieID = {movie_id}")
            print('A movie is successfully removed.')
            print()



# Problem 5 (3 pt.)
def insert_audience(cursor):
    # YOUR CODE GOES HERE
    name = input('Audience name: ')
    gender = input('Audience gender: ')
    age = input('Audience age: ')

    if name.isdigit():
        print("[ERROR] Name should be a string.")
        return
    if not (gender == "F" or gender == "M"):
        print("[ERROR] Gender should be either F or M.")
        return
    if not age.isdigit():
        print("[ERROR] Age should be an integer.")
        return

    try:
        cursor.execute(f"SELECT EXISTS (SELECT * FROM Audience WHERE name = '{name}' AND gender = '{gender}' AND age = {age})")
    except mysql.connector.Error as e:
        print("[ERROR] " + e.msg)
        return


    for (bool,) in cursor:
        if bool == 0: # only insert when audience is unique
            try:
                cursor.execute(f"INSERT INTO Audience VALUES(NULL, '{name}', '{gender}', {age})")
                print('An audience is successfully inserted')
                print()
                return
            except mysql.connector.Error as e:
                print("[ERROR] " + e.msg)

        else:
            print("The audience already exists!")
            return



# Problem 7 (4 pt.)
def remove_audience(cursor):
    audience_id = input('Audience ID: ')

    if not audience_id.isdigit():
        print("[ERROR] Please insert correct integer value.")
        return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Audience WHERE audienceID = {audience_id})")

    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Audience {audience_id} does not exist.')
            return
        if bool == 1:
            cursor.execute(f"DELETE FROM Booking WHERE audienceID = {audience_id}")
            cursor.execute(f"DELETE FROM Audience WHERE audienceID = {audience_id}")
            print('An audience is successfully removed.')
            print()
            return


# Problem 8 (5 pt.)
def book_movie(cursor):
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')

    if not (movie_id.isdigit() and audience_id.isdigit()):
        print("[ERROR] Please insert correct integer value.")
        return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Movie WHERE movieID = {movie_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Movie {movie_id} does not exist.')
            return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Audience WHERE audienceID = {audience_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Audience {audience_id} does not exist. Please insert correct Audience ID.')
            return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Booking WHERE movieID = {movie_id} and audienceID = {audience_id})")
    for (bool,) in cursor:
            if bool == 1:
                print('[ERROR] One audience cannot book the same movie twice.')
                return


    cursor.execute(f"INSERT INTO Booking VALUES (NULL, {movie_id}, {audience_id}, NULL)")
    print('Successfully booked a movie')
    print()
    return


# Problem 9 (5 pt.)
def rate_movie(cursor):
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')
    rating = input('Ratings (1~5): ')

    if not (movie_id.isdigit() and audience_id.isdigit() and rating.isdigit()):
        print("[ERROR] Inputs should be integers.")
        return


    cursor.execute(f"SELECT EXISTS (SELECT * FROM Movie WHERE movieID = {movie_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Movie {movie_id} does not exist.')
            return


    cursor.execute(f"SELECT EXISTS (SELECT * FROM Audience WHERE audienceID = {audience_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Audience {audience_id} does not exist')
            return

    if int(rating) > 5 or int(rating) < 1:
        print('[ERROR] Wrong value for a rating: should be a value between 1 to 5.')
        return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Booking WHERE movieID = {movie_id} and audienceID = {audience_id})")
    for (bool,) in cursor:
        if bool == 0:
            print("[ERROR] Booking does not exist. You can't rate the movie you haven't booked.")
            return
        else:
            cursor.execute(
                f"UPDATE Booking SET rating = {rating} WHERE movieID = {movie_id} and audienceID = {audience_id}")
            print('Successfully rated a movie.')
            print()


    # cursor.execute(f"SELECT EXISTS (SELECT * FROM Rating WHERE bookingID = (SELECT bookingID FROM Booking WHERE movieID = {movie_id} and audienceID = {audience_id}))")
    # for (bool,) in cursor:
    #     if bool == 0:
    #         cursor.execute(f"SELECT bookingID FROM Booking WHERE movieID = {movie_id} and audienceID = {audience_id}")
    #         for (bool,) in cursor:
    #             cursor.execute(f"INSERT INTO Rating VALUES ({bool}, {rating})")
    #             print('Successfully rated a movie.')
    #             return
    #     else:
    #         cursor.execute(f"UPDATE Rating SET rating = {rating} WHERE bookingID = (SELECT bookingID FROM Booking WHERE movieID = {movie_id} and audienceID = {audience_id})")
    #         print('Successfully rated a movie.')
    #         return


# Problem 10 (5 pt.)
def print_audiences_for_movie(cursor):
    movie_id = input('Movie ID: ')

    # check correct data type
    if not movie_id.isdigit():
        print("[ERROR] Movie ID should be an integer.")
        return

    #check if movie_id exists
    cursor.execute(f"SELECT EXISTS (SELECT * FROM Movie WHERE movieID = {movie_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Movie {movie_id} does not exist')
            return
        else:
            cursor.execute("SELECT a.audienceID, name, gender, age, rating FROM Audience a LEFT OUTER JOIN Booking b USING (audienceID)"
                           f"WHERE b.movieID = {movie_id}"
                           )
            print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
            print(
                "id" + " " * 5 + "name" + " " * 100 + "gender" + " " * 60 + "age" + " " * 9 + "rating" + " " * 9)
            print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
            for (audienceID, name, gender, age, rating) in cursor:
                print(
                    str(audienceID) + " " * (7 - len(str(audienceID))) + str(name) + " " * (104 - len(str(name))) + str(
                        gender)
                    + " " * (66 - len(str(gender))) + str(age) + " " * (12 - len(str(age))) + str(rating) + " " * (
                                15 - len(str(rating))))
            print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
            return




# Problem 11 (5 pt.)
def print_movies_for_audience(cursor):
    audience_id = input('Audience ID: ')
    if not audience_id.isdigit():
        print("[ERROR] Audience ID should be an integer.")
        return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Audience WHERE audienceID = {audience_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f'[ERROR] Audience {audience_id} does not exist')
            return
        else:
            cursor.execute("SELECT m.movieID, title, directorName, price, rating FROM Movie m LEFT OUTER JOIN Booking b USING (movieID)"
                           f"WHERE b.audienceID = {audience_id}")
            print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
            print(
                "id" + " " * 5 + "title" + " " * 100 + "director" + " " * 55 + "price" + " " * 8 + "rating" + " " * 8)
            print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))

            for (movieID, title, director, price, rating) in cursor:
                print(str(movieID) + " " * (7 - len(str(movieID))) + str(title) + " " * (105 - len(str(title))) + str(
                    director) + " " * (63 - len(str(director))) + str(price) + " " * (13 - len(str(price))) + str(rating) + " " * (14 - len(str(rating))))
            print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
            return




# Problem 12 (10 pt.)
def recommend(cursor):
    audience_id = input('Audience ID: ')
    if not audience_id.isdigit():
        print("[ERROR] Audience ID should be an integer.")
        return

    cursor.execute(f"SELECT EXISTS (SELECT * FROM Audience WHERE audienceID={audience_id})")
    for (bool,) in cursor:
        if bool == 0:
            print(f"[ERROR] Audience ID {audience_id} does not exist.")
            return


    cursor.execute("SELECT movieID FROM Movie ORDER BY movieID DESC LIMIT 1")
    row_col = [movieID for (movieID,) in cursor]
    cursor.execute("SELECT audienceID FROM Audience ORDER BY audienceID DESC LIMIT 1")
    row_col += [audienceID for (audienceID,) in cursor]
    movie_num, audience_num = row_col[0], row_col[1]
    user_item = [[0 for i in range(movie_num)] for j in range(audience_num)]

    cursor.execute("SELECT EXISTS (SELECT * FROM Booking WHERE rating IS NOT NULL)")
    for (bool,) in cursor:
        if bool == 0:
            print('[ERROR] Rating does not exist.')
            return

    cursor.execute("SELECT audienceID, movieID, rating FROM Audience CROSS JOIN Movie LEFT OUTER JOIN Booking USING (audienceID, movieID)"
                    "ORDER BY audienceID, movieID"
                   )

    # initialise user_item array
    for (audienceID, movieID, rating) in cursor:
        if rating is not None:
            user_item[audienceID-1][movieID-1] = rating

    non_rated_movie = []
    for i in range(movie_num):
        if user_item[int(audience_id)][i] == 0:
            non_rated_movie.append(i)


    # temporary rating for empty ratings
    for i in range(audience_num):
        if sum(user_item[i]) > 0:
            result, count = 0, 0
            for j in range(movie_num):
                if user_item[i][j] > 0:
                    result += user_item[i][j]
                    count += 1
            if count != 0:
                avg = result / count
                for j in range(movie_num):
                    if user_item[i][j] == 0:
                        user_item[i][j] = avg

    similarity_matrix = [[0 for i in range(audience_num)] for j in range(audience_num)]
    for i in range(audience_num):
        for j in range(audience_num):
            if i == j:
                similarity_matrix[i][j] = 1.0
            else:
                numerator, distanceA, distanceB = 0, 0, 0
                for k in range(movie_num):
                    numerator += user_item[i][k] * user_item[j][k]
                    distanceA += user_item[i][k] ** 2
                    distanceB += user_item[j][k] ** 2
                if distanceA == 0 or distanceB == 0:
                    cos_sim = 0
                else:
                    cos_sim = round(numerator / (math.sqrt(distanceA) * math.sqrt(distanceB)), 2)
                similarity_matrix[i][j] = cos_sim
    rating = []
    for k in non_rated_movie:
        numerator, denominator = 0, 0
        for i in range(audience_num):
            numerator += user_item[i][k] * similarity_matrix[int(audience_id)][i]
            denominator += similarity_matrix[int(audience_id)][i]
        rating.append(numerator / denominator)

    max_rating_movie = 0
    expected_rating = 0
    for i in range(len(rating)):
        if rating[i] > expected_rating:
            max_rating_movie = non_rated_movie[i]
            expected_rating = rating[i]

    avg_rating = user_item[int(audience_id)][max_rating_movie]

    cursor.execute(f"SELECT movieID, title, directorName, price FROM Movie WHERE movieID = {max_rating_movie+1}")
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))
    print(
        "id" + " " * 4 + "title" + " " * 96 + "director" + " " * 44 + "price" + " " * 4 + "avg. rating" + " " * 4 + "expected rating" + " " * 4)
    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))

    for (id, title, director, price) in cursor:
        print(str(id) + " " * (6 - len(str(id))) + str(title) + " " * (101 - len(str(title))) + str(
            director) + " " * (52 - len(str(director))) + str(price) + " " * (9 - len(str(price))) + str(
            avg_rating) + " " * (15 - len(str(avg_rating))) + str(expected_rating) + " " * (19-len(str(round(expected_rating,2)))))

    print("-" * (2+5+5+100+8+50+5+4+8+3+7+5))


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
    reset(cursor, "Y")

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
        menu = input('Select your action: ')
        if not menu.isdigit():
            print("[ERROR] Invalid action.")
            continue
        menu = int(menu)
        if menu == 1:
            print_movies(cursor)
        elif menu == 2:
            print_audiences(cursor)
        elif menu == 3:
            insert_movie(cursor)
        elif menu == 4:
            remove_movie(cursor)
        elif menu == 5:
            insert_audience(cursor)
        elif menu == 6:
            remove_audience(cursor)
        elif menu == 7:
            book_movie(cursor)
        elif menu == 8:
            rate_movie(cursor)
        elif menu == 9:
            print_audiences_for_movie(cursor)
        elif menu == 10:
            print_movies_for_audience(cursor)
        elif menu == 11:
            recommend(cursor)
        elif menu == 12:
            print('Bye!')
            cursor.close()
            connection.close()
            break
        elif menu == 13:
            inputVal = input("Are you sure to reset the database? [Y/N] ")
            if inputVal != 'Y' and inputVal != 'N':
                print("[ERROR] Please choose between 'Y' or 'N'.")
                return
            reset(cursor, inputVal)
        else:
            print('[ERROR] Invalid action.')


if __name__ == "__main__":
    main()