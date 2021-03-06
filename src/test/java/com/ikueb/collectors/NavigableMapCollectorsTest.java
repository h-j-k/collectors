/*
 * Copyright 2017 h-j-k. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ikueb.collectors;

import org.testng.annotations.Test;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.ikueb.collectors.NavigableMapCollectors.*;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonMap;
import static java.util.Comparator.naturalOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.testng.Assert.fail;

public class NavigableMapCollectorsTest {

    @SafeVarargs
    private static <K, V> Map<K, List<V>> toMap(K key, V... values) {
        return singletonMap(key, stream(values).collect(Collectors.toList()));
    }

    @SafeVarargs
    private static <T extends Comparable<T>> NavigableMap<T, List<T>> combineToNavigableMap(
            Map<T, List<T>>... maps) {
        return combineToNavigableMap(naturalOrder(), maps);
    }

    @SafeVarargs
    private static <T> NavigableMap<T, List<T>> combineToNavigableMap(
            Comparator<? super T> comparator, Map<T, List<T>>... maps) {
        NavigableMap<T, List<T>> results = new TreeMap<>(comparator);
        stream(maps).forEach(results::putAll);
        return results;
    }

    @SafeVarargs
    private static <T extends Comparable<T>> NavigableMap<T, T> asNavigableMap(T... values) {
        return asNavigableMap(naturalOrder(), values);
    }

    @SafeVarargs
    private static <T> NavigableMap<T, T> asNavigableMap(Comparator<? super T> comparator,
            T... values) {
        if (values.length % 2 != 0) {
            throw new IllegalArgumentException("Input length must be even.");
        }
        Iterator<T> iterator = stream(values).iterator();
        NavigableMap<T, T> results = new TreeMap<>(comparator);
        while (iterator.hasNext()) {
            results.put(iterator.next(), iterator.next());
        }
        return results;
    }

    private static <K, V> void assertMapOrdering(NavigableMap<K, V> actual,
            NavigableMap<K, V> expected) {
        Iterator<Entry<K, V>> actualIterator = actual.entrySet().iterator();
        Iterator<Entry<K, V>> expectedIterator = expected.entrySet().iterator();
        while (actualIterator.hasNext() && expectedIterator.hasNext()) {
            assertThat(actualIterator.next(), equalTo(expectedIterator.next()));
        }
        if (actualIterator.hasNext() || expectedIterator.hasNext()) {
            fail("Not expecting any more test values.");
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDuplicateKeyMappingThrows() {
        IntStream.range(0, 4).boxed().collect(toNavigableMap(i -> 0));
    }

    @Test
    public void testKeyMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toNavigableMap(i -> 4 - i)),
                            asNavigableMap(1, 3, 2, 2, 3, 1, 4, 0));
    }

    @Test
    public void testKeyValueMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toNavigableMap(i -> 4 - i,
                                                Math::incrementExact)),
                            asNavigableMap(1, 4, 2, 3, 3, 2, 4, 1));
    }

    @Test
    public void testKeyValueMappingWithKeyComparison() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toNavigableMap(i -> 4 - i,
                                                Math::decrementExact,
                                                Comparator.reverseOrder())),
                            asNavigableMap(Comparator.reverseOrder(),
                                            1, 2, 2, 1, 3, 0, 4, -1));
    }

    @Test
    public void testKeyValueMappingWithKeyComparisonAndValueMerger() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toNavigableMap(i -> i % 2,
                                                i -> i * (-1),
                                                Comparator.reverseOrder(),
                                                Integer::sum)),
                            asNavigableMap(Comparator.reverseOrder(),
                                            1, -4, 0, -2));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDuplicateConcurrentKeyMappingThrows() {
        IntStream.range(0, 4).boxed().collect(toNavigableMap(i -> 0));
    }

    @Test
    public void testConcurrentKeyMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toConcurrentNavigableMap(i -> 4 - i)),
                            asNavigableMap(1, 3, 2, 2, 3, 1, 4, 0));
    }

    @Test
    public void testConcurrentKeyValueMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toConcurrentNavigableMap(i -> 4 - i,
                                                Math::incrementExact)),
                            asNavigableMap(1, 4, 2, 3, 3, 2, 4, 1));
    }

    @Test
    public void testConcurrentKeyValueMappingWithKeyComparison() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toConcurrentNavigableMap(i -> 4 - i,
                                                Math::decrementExact,
                                                Comparator.reverseOrder())),
                            asNavigableMap(Comparator.reverseOrder(),
                                            1, 2, 2, 1, 3, 0, 4, -1));
    }

    @Test
    public void testConcurrentKeyValueMappingWithKeyComparisonAndValueMerger() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(toConcurrentNavigableMap(i -> i % 2,
                                                i -> i * (-1),
                                                Comparator.reverseOrder(),
                                                Integer::sum)),
                            asNavigableMap(Comparator.reverseOrder(),
                                            1, -4, 0, -2));
    }

    @Test
    public void testGroupingKeyMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(groupAndSortBy(i -> i % 2)),
                            combineToNavigableMap(toMap(0, 0, 2), toMap(1, 1, 3)));
    }

    @Test
    public void testGroupingKeyValueMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(groupAndSortBy(i -> 1 + i % 2, i -> i * 2)),
                            combineToNavigableMap(toMap(1, 0, 4), toMap(2, 2, 6)));
    }

    @Test
    public void testGroupingKeyValueMappingWithComparisons() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(groupAndSortBy(i -> 2 + i % 2, i -> i * 3,
                                                Comparator.reverseOrder(),
                                                Comparator.reverseOrder())),
                            combineToNavigableMap(Comparator.reverseOrder(),
                                                    toMap(3, 9, 3), toMap(2, 6, 0)));
    }

    @Test
    public void testConcurrentGroupingKeyMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(groupAndSortByConcurrent(i -> i % 2)),
                            combineToNavigableMap(toMap(0, 0, 2), toMap(1, 1, 3)));
    }

    @Test
    public void testConcurrentGroupingKeyValueMapping() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(groupAndSortByConcurrent(i -> 1 + i % 2,
                                                i -> i * 2)),
                            combineToNavigableMap(toMap(1, 0, 4), toMap(2, 2, 6)));
    }

    @Test
    public void testConcurrentGroupingKeyValueMappingWithComparisons() {
        assertMapOrdering(IntStream.range(0, 4)
                                    .boxed()
                                    .collect(groupAndSortByConcurrent(i -> 2 + i % 2,
                                                i -> i * 3,
                                                Comparator.reverseOrder(),
                                                Comparator.reverseOrder())),
                            combineToNavigableMap(Comparator.reverseOrder(),
                                                    toMap(3, 9, 3), toMap(2, 6, 0)));
    }
}
