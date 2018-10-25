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
package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.graphalgo.ml.FeatureVectorsProc;
import org.neo4j.graphalgo.similarity.SimilarityResult;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class FeatureVectorExporter extends StatementApi {

    private Integer propertyId;

    public FeatureVectorExporter(GraphDatabaseAPI api, String propertyName) {
        super(api);

        propertyId = applyInTransaction(statement -> statement.tokenWrite().propertyKeyGetOrCreateForName(propertyName));
    }

    public void export(Stream<FeatureVectorsProc.FeatureVectorResult> featureVectors) {
        writeSequential(featureVectors);
    }

    private void writeSequential(Stream<FeatureVectorsProc.FeatureVectorResult> similarityPairs) {
        similarityPairs.forEach(this::export);
    }

    private void export(FeatureVectorsProc.FeatureVectorResult featureVectorResult) {
        applyInTransaction(statement -> {
            try {
                List<Double> featureVector = featureVectorResult.featureVector;
                statement.dataWrite().nodeSetProperty(
                        featureVectorResult.nodeId,
                        propertyId,
                        Values.doubleArray(featureVector.stream().mapToDouble(d -> d).toArray()));
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
            return null;
        });

    }
}
