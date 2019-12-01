from neo4j import GraphDatabase, basic_auth

# connection with authentication
driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=False)

# connection without authentication
# driver = GraphDatabase.driver("bolt://localhost", encrypted=False)

session = driver.session()
transaction = session.begin_transaction()

write_output = open("output.txt", "w")

# finished: 1, 2, 3, 4, 6, 7
# revisit: 5
# todo: 8

# 1.) List the first 20 actors in descending order of the number of films they acted in.
# OUTPUT: actor_name, number_of_films_acted_in

write_output.write("### Q1 ###\n")

result = transaction.run("""
    MATCH (a:Actor)-[:ACTS_IN]->(m:Movie)
    WITH a, collect(m) AS movies
    RETURN a.name, length(movies)
    ORDER BY length(movies) DESC
    LIMIT 20;
    """)
for record in result:
    write_output.write(record['a.name'] + ',' + str(record['length(movies)']) + '\n')

write_output.write("\n")

# 2.) List the titles of all movies with a review with at most 3 stars.
# OUTPUT: movie title

write_output.write("### Q2 ###\n")

result = transaction.run("""
    MATCH (u) -[r:RATED]-> (m:Movie) 
    WHERE r.stars <= 3 
    RETURN m.title as mt 
    """)
for record in result:
    write_output.write(record['mt'] + '\n')

write_output.write("\n")

# 3.) Find the movie with the largest cast, out of the list of movies that have a review.
# OUTPUT: movie_title, number_of_cast_members

write_output.write("### Q3 ###\n")

result = transaction.run("""
    MATCH (a:Actor)-[:ACTS_IN]->(m:Movie)
    MATCH (u)-[r:RATED]->(m:Movie)
    WITH collect(DISTINCT a) as actors, m
    RETURN m.title, length(actors)
    ORDER BY length(actors) DESC 
    LIMIT 1;  
    """)
for record in result:
    write_output.write(record['m.title'] + ', ' + str(record['length(actors)']) + '\n')

write_output.write("\n")

# 4.) Find all the actors who have worked with at least 3 different directors (regardless of how many movies
# they acted in). For example, 3 movies with one director each would satisfy this (provided the directors
# where different), but also a single movie with 3 directors would satisfy it as well.
# OUTPUT: actor_name, number_of_directors_he/she_has_worked_with

write_output.write("### Q4 ###\n")

result = transaction.run("""
    MATCH (actor:Actor)-[:ACTS_IN]->(movie:Movie)<-[:DIRECTED]-(director:Director) 
    WITH actor, collect(DISTINCT director) AS num_directors WHERE length(num_directors) >= 3 
    RETURN actor.name AS actor_name, length(num_directors) AS `num_directors`
    """)
for record in result:
    write_output.write(record['actor_name'] + ', ' + str(record['num_directors']) + '\n')

write_output.write("\n")

# 5.) The Bacon number of an actor is the length of the shortest path between the actor and Kevin Bacon
# in the "co-acting" graph. That is, Kevin Bacon has Bacon number 0; all actors who acted in the same
# movie as him have Bacon number 1; all actors who acted in the same film as some actor with Bacon
# number 1 have Bacon number 2, etc. List all actors whose Bacon number is exactly 2 (first name, last name).
# You can familiarize yourself with the concept, by visiting The Oracle of Bacon.
# OUTPUT: actor_name

write_output.write("### Q5 ###\n")

result = transaction.run("""
    MATCH (bacon2:Actor)-[:ACTS_IN]->(movie2:Movie)<-[:ACTS_IN]-(bacon1:Actor)-[:ACTS_IN]->(movie:Movie)<-[:ACTS_IN]-(bacon:Actor {name: 'Kevin Bacon'}) 
    RETURN bacon2.name  
    """)
for record in result:
    write_output.write(record['bacon2.name'] + '\n')

write_output.write("\n")

# 6.) List which genres have movies where Tom Hanks starred in.
# OUTPUT: genre

write_output.write("### Q6 ###\n")

result = transaction.run("""
    MATCH (a:Actor)-[:ACTS_IN]->(m:Movie)
    WHERE a.name = "Tom Hanks"
    RETURN DISTINCT m.genre;
    """)
for record in result:
    write_output.write(record['m.genre'] + '\n')

write_output.write("\n")

# 7.) Show which directors have directed movies in at least 2 different genres.
# OUTPUT: director name, number of genres

write_output.write("### Q7 ###\n")

result = transaction.run("""
    MATCH (d:Director)-[:DIRECTED]->(m:Movie)
    WITH collect(DISTINCT m.genre) as genres, d
    WHERE length(genres) > 1
    RETURN d.name, length(genres)
    ORDER BY length(genres) DESC
    """)
for record in result:
    write_output.write(record['d.name'] + ', ' + str(record['length(genres)']) + '\n')

write_output.write("\n")

# 8.) Show the top 5 pairs of actor, director combinations, in descending order of frequency of occurrence.
# OUTPUT: director's name, actors' name, number of times director directed said actor in a movie

write_output.write("### Q8 ###\n")

result = transaction.run("""
    MATCH (d:Director)-[:DIRECTED]->(m:Movie)
    WITH collect(DISTINCT m.genre) as genres, d
    WHERE length(genres) > 1
    RETURN d.name, length(genres)
    ORDER BY length(genres) DESC
    """)
# for record in result:
    # write_output.write(record['m.genre'] + '\n')
    # print(record['d.name'] + ', ' + str(record['length(genres)']))


transaction.close()
write_output.close()
session.close()
