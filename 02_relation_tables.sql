-- MOVIE-PEOPLE

CREATE TABLE movie_people (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    person_id INT REFERENCES people(id) ON DELETE CASCADE,
    role TEXT,          -- actor, director, writer, producer, composer, dop
    importance INT,     -- Star1 = 1, Star2 = 2, etc.
    PRIMARY KEY (movie_id, person_id, role)
);

-- MOVIE-GENRES

CREATE TABLE movie_genres (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    genre_id INT REFERENCES genres(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, genre_id)
);

-- MOVIE-KEYWORDS

CREATE TABLE movie_keywords (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    keyword_id INT REFERENCES keywords(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, keyword_id)
);

-- MOVIE-PRODUCTION COMPANIES

CREATE TABLE movie_companies (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    company_id INT REFERENCES companies(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, company_id)
);

-- MOVIE-PRODUCTION COUNTRIES

CREATE TABLE movie_countries (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    country_id INT REFERENCES countries(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, country_id)
);

-- MOVIE-SPOKEN LANGUAGES

CREATE TABLE movie_languages (
    movie_id BIGINT REFERENCES movies(id) ON DELETE CASCADE,
    language_id INT REFERENCES languages(id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, language_id)
);


