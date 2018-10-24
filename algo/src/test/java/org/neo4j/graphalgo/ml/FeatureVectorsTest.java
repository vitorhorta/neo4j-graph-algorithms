package org.neo4j.graphalgo.ml;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class FeatureVectorsTest {

    @Test
    public void singleCategorySelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Map<String,Object>> weights = new ArrayList<>();
        weights.add(MapUtil.genericMap("id", "Italian", "weight",  3));

        assertEquals(asList(3D, 0D, 0D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void noCategoriesSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");

        assertEquals(asList(0D, 0D, 0D), new FeatureVectors().featureVector(values, Collections.emptyList()));
    }

    @Test
    public void moreThanOneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Map<String,Object>> weights = new ArrayList<>();
        weights.add(MapUtil.genericMap("id", "Italian", "weight",  3));
        weights.add(MapUtil.genericMap("id", "Chinese", "weight",  2));

        assertEquals(asList(3D, 0D, 2D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void allSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Map<String,Object>> weights = new ArrayList<>();
        weights.add(MapUtil.genericMap("id", "Italian", "weight",  3));
        weights.add(MapUtil.genericMap("id", "Indian", "weight",  5));
        weights.add(MapUtil.genericMap("id", "Chinese", "weight",  2));

        assertEquals(asList(3D, 5D, 2D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void nonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Map<String,Object>> weights = new ArrayList<>();
        weights.add(MapUtil.genericMap("id", "British", "weight",  5));

        assertEquals(asList(0D, 0D, 0D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void oneNonExistentSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        List<Map<String,Object>> weights = new ArrayList<>();
        weights.add(MapUtil.genericMap("id", "Chinese", "weight",  2));
        weights.add(MapUtil.genericMap("id", "British", "weight",  5));

        assertEquals(asList(0D, 0D, 2D), new FeatureVectors().featureVector(values, weights));
    }

    @Test
    public void nullSelectedMeansNoneSelected() {
        List<Object> values = asList("Italian", "Indian", "Chinese");
        assertEquals(asList(0D, 0D, 0D), new FeatureVectors().featureVector(values, null));
    }

    @Test
    public void nullAvailableMeansEmptyArray() {
        assertEquals(Collections.emptyList(), new FeatureVectors().featureVector(null, null));
    }

}