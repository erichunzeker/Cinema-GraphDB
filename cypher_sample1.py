from neo4j import GraphDatabase, basic_auth

# connection with authentication
driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=False)

# connection without authentication
# driver = GraphDatabase.driver("bolt://localhost", encrypted=False)

session = driver.session()
transaction = session.begin_transaction()

write_output = open("output.txt", "w")

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
    # print(record['a.name'], ',', record['length(movies)'])

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
    print(record['mt'])

write_output.write("\n")

# 3.) Find the movie with the largest cast, out of the list of movies that have a review.
# OUTPUT: movie_title, number_of_cast_members

write_output.write("### Q3 ###\n")

result = transaction.run("""
    MATCH (u) -[r:RATED]-> (m:Movie) 
    WHERE r.stars <= 3 
    RETURN m.title as mt 
    """)
for record in result:
    write_output.write(record['mt'] + '\n')
    print(record['mt'])

write_output.write("\n")

transaction.close()
write_output.close()
session.close()
