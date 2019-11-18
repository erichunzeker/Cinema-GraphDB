from neo4j import GraphDatabase, basic_auth

# connection with authentication
driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "password"), encrypted=False)

# connection without authentication
# driver = GraphDatabase.driver("bolt://localhost", encrypted=False)

session = driver.session()
transaction = session.begin_transaction()

write_output = open("output.txt", "w")

result = transaction.run("MATCH (people:Person) RETURN people.name LIMIT 10")
for record in result:
    print(record['people.name'])

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
    line = record['a.name'] + ',' + str(record['length(movies)'])
    write_output.write(record['a.name'] + ',' + str(record['length(movies)']) + '\n')
    print(record['a.name'], ',', record['length(movies)'])
transaction.close()

# 2.) List the titles of all movies with a review with at most 3 stars.
# OUTPUT: movie title

write_output.close()
session.close()
