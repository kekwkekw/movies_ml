"""This program is used to create movies database that you want to create"""

import pandas as pd


def multiple_input(phrase):
    string = input("\n" + phrase + "\n")
    items = [i.rstrip().lstrip() for i in string.split(",")]
    string = "+".join(items)
    return string


print("Hello! Dream of the movie that never existed. What would it be?\n"
      "Remember to write with commas, if there are multiple values, and use official full names only")

pd.DataFrame(
    columns=["Title", "ReleaseYr", "ReleaseDate", "AgeRating", "Length", "Genres", "Directors",
             "Writers", "Stars", "Keywords", "ProductionCompanies", "Score", "NumberOfScores", "Budget",
             "Gross"]).to_csv("dream_movies.csv", index=False)


def make_a_movie():
    title = input("\nWhat would you name this movie?\n")
    release = input("\nWhere you want it to be released? Note: write date like the example   2020 May 2\n").split(" ")
    release_yr, release_date = release[0], release[1] + " " + release[2]
    age_rating = input(
        "\nWrite movie's age rating. Choose one of the following: G PG PG-13 R\n")
    length = input("\nHow long you want this movie to be? Example     1 hour 30 minutes\n")
    genres = multiple_input(
        "What genres will your movie include? Don't forget to include Animation, it that's an animated movie")
    directors = multiple_input("Who will be directors?")
    writers = multiple_input("Who will be writers?")
    stars = multiple_input("Who will be stars?")
    companies = multiple_input("What studios will make this movie?")
    budget = "".join(["$", input("\nMovie's budget (in dollars)?\n")])
    keywords = multiple_input("Write some phrases (up to 10), that describe your movie the best\n"
                              "example: superhero, shark, sequel, based on super sonic, liar reveal,"
                              "buddy, murder, n word, blood, noir")
    pd.DataFrame([title, release_yr, release_date, age_rating, length, genres,
                  directors, writers, stars, keywords, companies, None, None, budget, None]).T.to_csv(
        "dream_movies.csv", mode='a', header=False, index=False)
    another_one = input("\n\nNice! Want to make another one? (Write Yes or No)\n")
    if another_one.lower() == "yes":
        make_a_movie()


make_a_movie()
print("Good job! Go check its gross in preprocessing_and_predicting.ipynb")
