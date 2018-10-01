// tag::function[]
RETURN algo.similarity.overlap([1,2,3], [1,2,4,5]) AS similarity
// end::function[]

// tag::create-sample-graph[]

MERGE (fahrenheit451:Book {title:'Fahrenheit 451'})
MERGE (dune:Book {title:'dune'})
MERGE (hungerGames:Book {title:'The Hunger Games'})
MERGE (nineteen84:Book {title:'1984'})

MERGE (scienceFiction:Genre {name: "Science Fiction"})
MERGE (fantasy:Genre {name: "Fantasy"})
MERGE (dystopia:Genre {name: "Dystopia"})

MERGE (fahrenheit451)-[:HAS_GENRE]->(dystopia)
MERGE (fahrenheit451)-[:HAS_GENRE]->(scienceFiction)
MERGE (fahrenheit451)-[:HAS_GENRE]->(fantasy)

MERGE (hungerGames)-[:HAS_GENRE]->(scienceFiction)
MERGE (hungerGames)-[:HAS_GENRE]->(fantasy)
MERGE (hungerGames)-[:HAS_GENRE]->(romance)

MERGE (nineteen84)-[:HAS_GENRE]->(scienceFiction)
MERGE (nineteen84)-[:HAS_GENRE]->(dystopia)

MERGE (dune)-[:HAS_GENRE]->(scienceFiction)
MERGE (dune)-[:HAS_GENRE]->(fantasy)

// end::create-sample-graph[]

// tag::stream[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data)
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, intersection, similarity
ORDER BY similarity DESC
// end::stream[]

// tag::stream-similarity-cutoff[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data, {similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, intersection, similarity
ORDER BY similarity DESC
// end::stream-similarity-cutoff[]

// tag::stream-topk[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard.stream(data, {topK: 1, similarityCutoff: 0.0})
YIELD item1, item2, count1, count2, intersection, similarity
RETURN algo.getNodeById(item1).name AS from, algo.getNodeById(item2).name AS to, similarity
ORDER BY from
// end::stream-topk[]

// tag::write-back[]
MATCH (p:Person)-[:LIKES]->(cuisine)
WITH {item:id(p), categories: collect(id(cuisine))} as userData
WITH collect(userData) as data
CALL algo.similarity.jaccard(data, {topK: 1, similarityCutoff: 0.1, write:true})
YIELD nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, stdDev, p25, p50, p75, p90, p95, p99, p999, p100
RETURN nodes, similarityPairs, write, writeRelationshipType, writeProperty, min, max, mean, p95
// end::write-back[]

// tag::query[]
MATCH (p:Person {name: "Praveena"})-[:SIMILAR]->(other),
      (other)-[:LIKES]->(cuisine)
WHERE not((p)-[:LIKES]->(cuisine))
RETURN cuisine.name AS cuisine
// end::query[]
