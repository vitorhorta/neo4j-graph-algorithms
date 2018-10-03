// tag::create-sample-graph[]

MERGE (zhen:Person {name: "Zhen"})
SET zhen.embedding = [0,1,1,0,1]

MERGE (praveena:Person {name: "Praveena"})
SET praveena.embedding = [1,0,0,1,1]

MERGE (michael:Person {name: "Michael"})
SET michael.embedding = []

MERGE (arya:Person {name: "Arya"})
SET arya.embedding = []

// end::create-sample-graph[]

// tag::one-hot-encoding-query[]
MATCH (cuisine:Cuisine)
WITH cuisine ORDER BY cuisine.name
WITH collect(cuisine) AS cuisines
MATCH (p:Person)
RETURN p.name AS person,
       algo.ml.oneHotEncoding(cuisines, [(p)-[:LIKES]->(cuisine) | cuisine]) AS encoding
ORDER BY person
// end::one-hot-encoding-query[]
