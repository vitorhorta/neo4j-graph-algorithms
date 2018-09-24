/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 * <p>
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 * <p>
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.impl;

import info.debatty.java.lsh.LSHMinHash;
import info.debatty.java.lsh.LSHSuperBit;
import org.junit.Test;
import scala.util.Random;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class LSHTest {

    @Test
    public void should() throws Exception {
        // R^n
        int n = 100;

        int stages = 2;
        int buckets = 10;

        String csvFile = "/Users/markneedham/neo/neo4j-graph-algorithms/medium_glove.txt";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = " ";

        Map<String, double[]> vectors = new HashMap<>();
        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] row = line.split(cvsSplitBy);
                String token = row[0];
                double [] vector = new double[row.length -1];
                for (int i = 1; i < vector.length; i++) {
                    double v = Double.valueOf(row[i]);
                    vector[i-1] = v;
                }
                vectors.put(token, vector);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        LSHSuperBit lsh = new LSHSuperBit(stages, buckets, n);

        Map<Integer, Integer> bucketCount = new HashMap<>();

        // Compute a SuperBit signature, and a LSH hash
        for (double[] vector : vectors.values()) {
            int[] hash = lsh.hash(vector);
            int c = bucketCount.getOrDefault(hash[0], 0);
            bucketCount.put(hash[0], c + 1);
        }
        System.out.println("bucketCount = " + bucketCount);
    }
}
