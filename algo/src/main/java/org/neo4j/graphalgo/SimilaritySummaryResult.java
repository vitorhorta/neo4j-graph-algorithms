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
package org.neo4j.graphalgo;

public class SimilaritySummaryResult {

    public final long nodes;
    public final long similarityPairs;
    public final double percentile50;
    public final double percentile75;
    public final double percentile90;
    public final double percentile99;
    public final double percentile999;
    public final double percentile100;

    public SimilaritySummaryResult(long nodes, long similarityPairs,
                                   double percentile50, double percentile75, double percentile90, double percentile99,
                                   double percentile999, double percentile100) {
        this.nodes = nodes;
        this.similarityPairs = similarityPairs;
        this.percentile50 = percentile50;
        this.percentile75 = percentile75;
        this.percentile90 = percentile90;
        this.percentile99 = percentile99;
        this.percentile999 = percentile999;
        this.percentile100 = percentile100;
    }
}
