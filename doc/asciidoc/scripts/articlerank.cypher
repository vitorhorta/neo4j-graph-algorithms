// tag::create-sample-graph[]

MERGE (home:Page {name:'Home'})
MERGE (about:Page {name:'About'})
MERGE (product:Page {name:'Product'})
MERGE (links:Page {name:'Links'})
MERGE (a:Page {name:'Site A'})
MERGE (b:Page {name:'Site B'})
MERGE (c:Page {name:'Site C'})
MERGE (d:Page {name:'Site D'})

MERGE (home)-[:LINKS]->(about)
MERGE (about)-[:LINKS]->(home)
MERGE (product)-[:LINKS]->(home)
MERGE (home)-[:LINKS]->(product)
MERGE (links)-[:LINKS]->(home)
MERGE (home)-[:LINKS]->(links)
MERGE (links)-[:LINKS]->(a)
MERGE (a)-[:LINKS]->(home)
MERGE (links)-[:LINKS]->(b)
MERGE (b)-[:LINKS]->(home)
MERGE (links)-[:LINKS]->(c)
MERGE (c)-[:LINKS]->(home)
MERGE (links)-[:LINKS]->(d)
MERGE (d)-[:LINKS]->(home)

// end::create-sample-graph[]

// tag::stream-sample-graph[]

CALL algo.articleRank.stream('Page', 'LINKS', {iterations:20, dampingFactor:0.85})
YIELD nodeId, score
RETURN algo.getNodeById(nodeId).name AS page,score
ORDER BY score DESC

// end::stream-sample-graph[]

// tag::write-sample-graph[]

CALL algo.articleRank('Page', 'LINKS',
  {iterations:20, dampingFactor:0.85, write: true,writeProperty:"pagerank"})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty

// end::write-sample-graph[]

// tag::create-sample-weighted-graph[]

MERGE (home:Page {name:'Home'})
MERGE (about:Page {name:'About'})
MERGE (product:Page {name:'Product'})
MERGE (links:Page {name:'Links'})
MERGE (a:Page {name:'Site A'})
MERGE (b:Page {name:'Site B'})
MERGE (c:Page {name:'Site C'})
MERGE (d:Page {name:'Site D'})

MERGE (home)-[:LINKS {weight: 0.2}]->(about)
MERGE (home)-[:LINKS {weight: 0.2}]->(links)
MERGE (home)-[:LINKS {weight: 0.6}]->(product)

MERGE (about)-[:LINKS {weight: 1.0}]->(home)

MERGE (product)-[:LINKS {weight: 1.0}]->(home)

MERGE (a)-[:LINKS {weight: 1.0}]->(home)

MERGE (b)-[:LINKS {weight: 1.0}]->(home)

MERGE (c)-[:LINKS {weight: 1.0}]->(home)

MERGE (d)-[:LINKS {weight: 1.0}]->(home)

MERGE (links)-[:LINKS {weight: 0.8}]->(home)
MERGE (links)-[:LINKS {weight: 0.05}]->(a)
MERGE (links)-[:LINKS {weight: 0.05}]->(b)
MERGE (links)-[:LINKS {weight: 0.05}]->(c)
MERGE (links)-[:LINKS {weight: 0.05}]->(d)

// end::create-sample-weighted-graph[]

// tag::cypher-loading[]

CALL algo.articleRank(
  'MATCH (p:Page) RETURN id(p) as id',
  'MATCH (p1:Page)-[:Link]->(p2:Page) RETURN id(p1) as source, id(p2) as target',
  {graph:'cypher', iterations:5, write: true}
)

// end::cypher-loading[]


// tag::huge-projection[]

CALL algo.articleRank('Page','LINKS',
  {graph:'huge'})
YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, writeProperty;

// end::huge-projection[]
