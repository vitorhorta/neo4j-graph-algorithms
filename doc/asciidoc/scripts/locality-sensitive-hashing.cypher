// tag::create-sample-graph[]

MERGE (zhen:Person {name: "Zhen"})
SET zhen.embedding = [0,1,1,0,1]

MERGE (praveena:Person {name: "Praveena"})
SET praveena.embedding = [1,0,0,1,1]

MERGE (michael:Person {name: "Michael"})
SET michael.embedding = [0.5,1,2,0,0.7]

MERGE (arya:Person {name: "Arya"})
SET arya.embedding = [0.3,1,4,2,1.5]

// end::create-sample-graph[]

// tag::one-hot-encoding-query[]
MATCH (person:Person)
WITH {item:id(person), weights: person.embedding} as userData
WITH collect(userData) as data
CALL algo.ml.lsh.stream(data)
YIELD nodeId, bucket
RETURN algo.getNodeById(nodeId).name AS from, bucket

// end::one-hot-encoding-query[]
