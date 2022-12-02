from mysql.connector import connect

# Problem 1 (5 pt.)
def reset():
    # YOUR CODE GOES HERE

    print('Initialized database')
    # YOUR CODE GOES HERE
    pass

# Problem 2 (3 pt.)
def print_movies():
    # YOUR CODE GOES HERE

    
    # YOUR CODE GOES HERE
    pass

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
    #connect to mysql
    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2022_81863',
        password='DB2022_81863',
        db='DB2022_81863',
        charset='utf8'

    )
    tables = {}

    #create table Movie
    tables['Movie'] = (
        "CREATE TABLE 'Movie' ("
        "    'movieID' int NOT NULL AUTO_INCREMENT"
        "    'title' varchar(100) NOT NULL"
        "    'price' int"
        "    PRIMARY KEY ('title')"
        "    FOREIGN KEY ('directorName') REFERENCES 'Director' ('Name')"
    )

    #create table Booking
    tables['Booking'] = (
        "CREATE TABLE 'Booking' ("
        "    'bookingID' int NOT_NULL"
    )




    # initialize database
    
    reset()

    while True:
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
            print_movies()
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
            break
        elif menu == 13:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()