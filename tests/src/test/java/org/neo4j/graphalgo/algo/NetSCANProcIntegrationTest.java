/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.algo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphalgo.NetSCANProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class NetSCANProcIntegrationTest {

    private static final String DB_CYPHER = "" +
            "CREATE (p1:`Person`:`UNIQUE IMPORT LABEL` {`id`:1, `UNIQUE IMPORT ID`:0})\n" +
            "CREATE (p2:`Person`:`UNIQUE IMPORT LABEL` {`id`:2, `UNIQUE IMPORT ID`:1})\n" +
            "CREATE (p3:`Person`:`UNIQUE IMPORT LABEL` {`id`:3, `UNIQUE IMPORT ID`:2})\n" +
            "CREATE (p4:`Person`:`UNIQUE IMPORT LABEL` {`id`:4, `UNIQUE IMPORT ID`:3})\n" +
            "CREATE (p5:`Person`:`UNIQUE IMPORT LABEL` {`id`:5, `UNIQUE IMPORT ID`:4})\n" +
            "CREATE (p6:`Person`:`UNIQUE IMPORT LABEL` {`id`:6, `UNIQUE IMPORT ID`:5})\n" +
            "CREATE (p7:`Person`:`UNIQUE IMPORT LABEL` {`id`:7, `UNIQUE IMPORT ID`:6})\n" +
            "CREATE (p8:`Person`:`UNIQUE IMPORT LABEL` {`id`:8, `UNIQUE IMPORT ID`:7})\n" +
            "CREATE (p9:`Person`:`UNIQUE IMPORT LABEL` {`id`:9, `UNIQUE IMPORT ID`:8})\n" +
            "CREATE (p10:`Person`:`UNIQUE IMPORT LABEL` {`id`:10, `UNIQUE IMPORT ID`:9})\n" +
            "CREATE (p11:`Person`:`UNIQUE IMPORT LABEL` {`id`:11, `UNIQUE IMPORT ID`:10})\n" +
            "CREATE (p12:`Person`:`UNIQUE IMPORT LABEL` {`id`:12, `UNIQUE IMPORT ID`:11})\n" +
            "CREATE (p13:`Person`:`UNIQUE IMPORT LABEL` {`id`:13, `UNIQUE IMPORT ID`:12})\n" +
            "CREATE (p14:`Person`:`UNIQUE IMPORT LABEL` {`id`:14, `UNIQUE IMPORT ID`:13})\n" +
            "CREATE (p15:`Person`:`UNIQUE IMPORT LABEL` {`id`:15, `UNIQUE IMPORT ID`:14})\n" +
            "CREATE (p16:`Person`:`UNIQUE IMPORT LABEL` {`id`:16, `UNIQUE IMPORT ID`:15})\n" +
            "CREATE (p17:`Person`:`UNIQUE IMPORT LABEL` {`id`:17, `UNIQUE IMPORT ID`:16})\n" +
            "CREATE (p18:`Person`:`UNIQUE IMPORT LABEL` {`id`:18, `UNIQUE IMPORT ID`:17})\n" +
            "CREATE (p19:`Person`:`UNIQUE IMPORT LABEL` {`id`:19, `UNIQUE IMPORT ID`:18})\n" +
            "CREATE (p20:`Person`:`UNIQUE IMPORT LABEL` {`id`:20, `UNIQUE IMPORT ID`:19})\n" +
            "CREATE (p21:`Person`:`UNIQUE IMPORT LABEL` {`id`:21, `UNIQUE IMPORT ID`:20})\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.5714285714285714}]->(p21)\n" +
            "CREATE (p11)-[:`LIKES` {`weight`:1}]->(p10)\n" +
            "CREATE (p18)-[:`LIKES` {`weight`:0.8571428571428571}]->(p19)\n" +
            "CREATE (p15)-[:`LIKES` {`weight`:0.8571428571428571}]->(p19)\n" +
           // "CREATE (p16)-[:`LIKES` {`weight`:1}]->(p2)\n" +
            "CREATE (p2)-[:`LIKES` {`weight`:1}]->(p1)\n" +
            "CREATE (p3)-[:`LIKES` {`weight`:0.5}]->(p4)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:1}]->(p16)\n" +
            "CREATE (p20)-[:`LIKES` {`weight`:1}]->(p18)\n" +
            "CREATE (p11)-[:`LIKES` {`weight`:0.4444444444444444}]->(p9)\n" +
            "CREATE (p7)-[:`LIKES` {`weight`:0.375}]->(p6)\n" +
            "CREATE (p5)-[:`LIKES` {`weight`:0.333}]->(p8)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.666}]->(p4)\n" +
            "CREATE (p21)-[:`LIKES` {`weight`:1}]->(p20)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:1}]->(p17)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:1}]->(p17)\n" +
            "CREATE (p18)-[:`LIKES` {`weight`:0.8571}]->(p15)\n" +
            "CREATE (p3)-[:`LIKES` {`weight`:0.5}]->(p5)\n" +
            "CREATE (p16)-[:`LIKES` {`weight`:1}]->(p20)\n" +
            "CREATE (p12)-[:`LIKES` {`weight`:0.666}]->(p13)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.5}]->(p5)\n" +
            "CREATE (p4)-[:`LIKES` {`weight`:1}]->(p2)\n" +
            "CREATE (p21)-[:`LIKES` {`weight`:1}]->(p18)\n" +
            "CREATE (p20)-[:`LIKES` {`weight`:1}]->(p19)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.8}]->(p10)\n" +
            "CREATE (p21)-[:`LIKES` {`weight`:1}]->(p16)\n" +
            "CREATE (p13)-[:`LIKES` {`weight`:1}]->(p12)\n" +
            "CREATE (p18)-[:`LIKES` {`weight`:0.857}]->(p16)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:1}]->(p1)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.5714}]->(p19)\n" +
            "CREATE (p14)-[:`LIKES` {`weight`:0.75}]->(p12)\n" +
            "CREATE (p2)-[:`LIKES` {`weight`:0.5}]->(p6)\n" +
            "CREATE (p7)-[:`LIKES` {`weight`:0.5}]->(p4)\n" +
            "CREATE (p16)-[:`LIKES` {`weight`:1}]->(p19)\n" +
            "CREATE (p5)-[:`LIKES` {`weight`:1}]->(p3)\n" +
            "CREATE (p14)-[:`LIKES` {`weight`:0.5}]->(p13)\n" +
            "CREATE (p15)-[:`LIKES` {`weight`:1}]->(p18)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:1}]->(p4)\n" +
            "CREATE (p13)-[:`LIKES` {`weight`:1}]->(p14)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:1}]->(p3)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:1}]->(p13)\n" +
            "CREATE (p10)-[:`LIKES` {`weight`:1}]->(p11)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.555}]->(p9)\n" +
            "CREATE (p13)-[:`LIKES` {`weight`:1}]->(p10)\n" +
            "CREATE (p2)-[:`LIKES` {`weight`:0.5}]->(p4)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.5}]->(p7)\n" +
            "CREATE (p20)-[:`LIKES` {`weight`:0.857}]->(p15)\n" +
            "CREATE (p11)-[:`LIKES` {`weight`:1}]->(p14)\n" +
            "CREATE (p18)-[:`LIKES` {`weight`:0.857}]->(p20)\n" +
            "CREATE (p10)-[:`LIKES` {`weight`:0.75}]->(p12)\n" +
            "CREATE (p4)-[:`LIKES` {`weight`:0.75}]->(p6)\n" +
            "CREATE (p18)-[:`LIKES` {`weight`:0.857}]->(p21)\n" +
            "CREATE (p5)-[:`LIKES` {`weight`:0.857}]->(p1)\n" +
            "CREATE (p16)-[:`LIKES` {`weight`:0.857}]->(p17)\n" +
            "CREATE (p4)-[:`LIKES` {`weight`:0.857}]->(p2)\n" +
            "CREATE (p1)-[:`LIKES` {`weight`:0.857}]->(p2)\n" +
            "CREATE (p4)-[:`LIKES` {`weight`:0.857}]->(p7)\n" +
            "CREATE (p5)-[:`LIKES` {`weight`:0.857}]->(p6)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:0.857}]->(p8)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.857}]->(p11)\n" +
            "CREATE (p10)-[:`LIKES` {`weight`:0.857}]->(p13)\n" +
            "CREATE (p11)-[:`LIKES` {`weight`:0.857}]->(p12)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:0.857}]->(p21)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.857}]->(p6)\n" +
            "CREATE (p5)-[:`LIKES` {`weight`:0.857}]->(p4)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.857}]->(p13)\n" +
            "CREATE (p2)-[:`LIKES` {`weight`:0.857}]->(p5)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:0.857}]->(p18)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:0.857}]->(p2)\n" +
            "CREATE (p5)-[:`LIKES` {`weight`:0.857}]->(p2)\n" +
            "CREATE (p16)-[:`LIKES` {`weight`:0.857}]->(p21)\n" +
            "CREATE (p14)-[:`LIKES` {`weight`:0.857}]->(p11)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.857}]->(p20)\n" +
            "CREATE (p16)-[:`LIKES` {`weight`:0.857}]->(p17)\n" +
            "CREATE (p15)-[:`LIKES` {`weight`:0.857}]->(p20)\n" +
            "CREATE (p17)-[:`LIKES` {`weight`:0.857}]->(p18)\n" +
            "CREATE (p18)-[:`LIKES` {`weight`:0.857}]->(p17)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:0.857}]->(p5)\n" +
            "CREATE (p16)-[:`LIKES` {`weight`:0.857}]->(p15)\n" +
            "CREATE (p17)-[:`LIKES` {`weight`:0.857}]->(p20)\n" +
            "CREATE (p17)-[:`LIKES` {`weight`:0.857}]->(p19)\n" +
            "CREATE (p12)-[:`LIKES` {`weight`:0.857}]->(p14)\n" +
            "CREATE (p17)-[:`LIKES` {`weight`:0.857}]->(p21)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.857}]->(p11)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:0.857}]->(p20)\n" +
            "CREATE (p11)-[:`LIKES` {`weight`:0.857}]->(p13)\n" +
            "CREATE (p12)-[:`LIKES` {`weight`:0.857}]->(p10)\n" +
            "CREATE (p17)-[:`LIKES` {`weight`:0.857}]->(p16)\n" +
            "CREATE (p4)-[:`LIKES` {`weight`:0.857}]->(p5)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.857}]->(p16)\n" +
            "CREATE (p1)-[:`LIKES` {`weight`:0.857}]->(p5)\n" +
            "CREATE (p21)-[:`LIKES` {`weight`:0.857}]->(p15)\n" +
            "CREATE (p20)-[:`LIKES` {`weight`:0.857}]->(p17)\n" +
            "CREATE (p12)-[:`LIKES` {`weight`:0.857}]->(p11)\n" +
            "CREATE (p15)-[:`LIKES` {`weight`:0.857}]->(p16)\n" +
            "CREATE (p9)-[:`LIKES` {`weight`:0.857}]->(p8)\n" +
            "CREATE (p21)-[:`LIKES` {`weight`:0.857}]->(p17)\n" +
            "CREATE (p8)-[:`LIKES` {`weight`:0.857}]->(p10)\n" +
            "CREATE (p17)-[:`LIKES` {`weight`:0.857}]->(p15)\n" +
            "CREATE (p13)-[:`LIKES` {`weight`:0.857}]->(p11)\n" +
            "CREATE (p19)-[:`LIKES` {`weight`:0.857}]->(p15)\n" +
            "CREATE (p20)-[:`LIKES` {`weight`:0.857}]->(p16)\n" +
            "CREATE (p20)-[:`LIKES` {`weight`:0.857}]->(p21)\n" +
            "CREATE (p15)-[:`LIKES` {`weight`:0.857}]->(p21)\n" +
            "CREATE (p6)-[:`LIKES` {`weight`:1}]->(p7)\n";
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:7}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:5}) CREATE (n1)-[r:`LIKES` {`weight`:0.375}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:12}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:7}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:19}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:9}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:5}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.2222222222222222}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:10}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:7}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:9}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:7}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:5}) CREATE (n1)-[r:`LIKES` {`weight`:0.375}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:18}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:6}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:7}) CREATE (n1)-[r:`LIKES` {`weight`:0.3333333333333333}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:15}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:5}) CREATE (n1)-[r:`LIKES` {`weight`:0.25}]->(n2)\n" +
//            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:12}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:8}) CREATE (n1)-[r:`LIKES` {`weight`:0.4444444444444444}]->(n2)\n" +

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false},
                new Object[]{true}
        );
    }

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

//    private final boolean parallel;

//    public NetSCANProcIntegrationTest(boolean parallel) {
//        this.parallel = parallel;
//    }

    @Before
    public void setup() throws KernelException {
        db.resolveDependency(Procedures.class).registerProcedure(NetSCANProc.class);
        db.execute(DB_CYPHER);
    }


    @Test
    public void shouldStreamResults() {

        String query = "CALL algo.netscan.stream(null, 'LIKES', {iterations: 20, direction: 'INCOMING', partitionProperty: 'ns', concurrency: 1, minPts: 5, eps: 0.5, higherBetter: true}) " +
                "YIELD nodeId, label " +
                "MATCH (node) WHERE id(node) = nodeId " +
                "RETURN node.id AS id, id(node) AS internalNodeId, label";

        runQuery(query, row -> {
            System.out.println("row = " + row.get("id") + " " + row.get("label"));
        });
    }


    private void runQuery(
        String query,
        Consumer<Result.ResultRow> check) {
        runQuery(query, Collections.emptyMap(), check);
    }

    private void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

}
