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

import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.Comparator.naturalOrder;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;

/**
 * Utility class providing a set of {@link Collector} implementations that collect results
 * into {@link NavigableMap} objects.
 */
public final class NavigableMapCollectors {

    private NavigableMapCollectors() {
        // empty
    }

    /**
     * @return a {@link BinaryOperator} that throws {@link IllegalStateException}
     */
    private static <T> BinaryOperator<T> throwOnMerge() {
        return (a, b) -> {
            throw new IllegalStateException();
        };
    }

    /**
     * Simple one-to-one mapping of stream elements to their keys, given natural
     * ordering on the keys.
     *
     * @param keyMapper the {@link Function} to map a stream element as the target key
     * @return a {@link NavigableMap} with the mapped keys and the stream elements as
     * values
     */
    public static <T, A,
            K extends Comparable<K>,
            M extends NavigableMap<K, T>> Collector<T, A, M> toNavigableMap(
            Function<? super T, ? extends K> keyMapper) {
        return toNavigableMap(keyMapper, identity());
    }

    /**
     * Simple one-to-one mapping of stream elements to desired keys and values, assuming
     * natural ordering on the keys.
     *
     * @param keyMapper   the {@link Function} to map a stream element as the target key
     * @param valueMapper the {@link Function} to map a stream element as the target value
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    public static <T, A,
            K extends Comparable<K>, V,
            M extends NavigableMap<K, V>> Collector<T, A, M> toNavigableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper) {
        return toNavigableMap(keyMapper, valueMapper, naturalOrder());
    }

    /**
     * Simple one-to-one mapping of stream elements to desired keys and values, given a
     * {@link Comparator} for the keys.
     *
     * @param keyMapper     the {@link Function} to map a stream element as the target key
     * @param valueMapper   the {@link Function} to map a stream element as the target value
     * @param keyComparator the {@link Comparator} to use for the resulting
     *                      {@link NavigableMap}
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    public static <T, A, K, V,
            M extends NavigableMap<K, V>> Collector<T, A, M> toNavigableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Comparator<? super K> keyComparator) {
        return toNavigableMap(keyMapper, valueMapper, keyComparator, throwOnMerge());
    }

    /**
     * Maps stream elements to desired keys and values, given a {@link Comparator} for the
     * keys and a {@link BinaryOperator} for merging values on identical keys.
     *
     * @param keyMapper     the {@link Function} to map a stream element as the target key
     * @param valueMapper   the {@link Function} to map a stream element as the target value
     * @param keyComparator the {@link Comparator} to use for the resulting
     *                      {@link NavigableMap}
     * @param mergeOperator the {@link BinaryOperator} to use for merging values
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    @SuppressWarnings("unchecked")
    public static <T, A, K, V,
            M extends NavigableMap<K, V>> Collector<T, A, M> toNavigableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Comparator<? super K> keyComparator,
            BinaryOperator<V> mergeOperator) {
        return (Collector<T, A, M>) toMap(keyMapper, valueMapper, mergeOperator,
                () -> (M) new TreeMap<K, List<V>>(keyComparator));
    }

    /**
     * Simple one-to-one concurrent mapping of stream elements to their keys, assuming
     * natural ordering on the keys.
     *
     * @param keyMapper the {@link Function} to map a stream element as the target key
     * @return a {@link NavigableMap} with the mapped keys and the stream elements as
     * values
     */
    public static <T, A,
            K extends Comparable<K>,
            M extends ConcurrentNavigableMap<K, T>> Collector<T, A, M> toConcurrentNavigableMap(
            Function<? super T, ? extends K> keyMapper) {
        return toConcurrentNavigableMap(keyMapper, identity());
    }

    /**
     * Simple one-to-one concurrent mapping of stream elements to desired keys and values,
     * given natural ordering on the keys.
     *
     * @param keyMapper   the {@link Function} to map a stream element as the target key
     * @param valueMapper the {@link Function} to map a stream element as the target value
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    public static <T, A,
            K extends Comparable<K>, V,
            M extends ConcurrentNavigableMap<K, V>> Collector<T, A, M> toConcurrentNavigableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper) {
        return toConcurrentNavigableMap(keyMapper, valueMapper, naturalOrder());
    }

    /**
     * Simple one-to-one concurrent mapping of stream elements to desired keys and values,
     * given a {@link Comparator} for the keys.
     *
     * @param keyMapper     the {@link Function} to map a stream element as the target key
     * @param valueMapper   the {@link Function} to map a stream element as the target value
     * @param keyComparator the {@link Comparator} to use for the resulting
     *                      {@link NavigableMap}
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    public static <T, A, K, V,
            M extends ConcurrentNavigableMap<K, V>> Collector<T, A, M> toConcurrentNavigableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Comparator<? super K> keyComparator) {
        return toConcurrentNavigableMap(keyMapper, valueMapper,
                keyComparator, throwOnMerge());
    }

    /**
     * Performs a concurrent mapping of stream elements to desired keys and values, given
     * a {@link Comparator} for the keys and a {@link BinaryOperator} for merging values
     * on identical keys.
     *
     * @param keyMapper     the {@link Function} to map a stream element as the target key
     * @param valueMapper   the {@link Function} to map a stream element as the target value
     * @param keyComparator the {@link Comparator} to use for the resulting
     *                      {@link NavigableMap}
     * @param mergeOperator the {@link BinaryOperator} to use for merging values
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    @SuppressWarnings("unchecked")
    public static <T, A, K, V,
            M extends ConcurrentNavigableMap<K, V>> Collector<T, A, M> toConcurrentNavigableMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Comparator<? super K> keyComparator,
            BinaryOperator<V> mergeOperator) {
        return (Collector<T, A, M>) toConcurrentMap(keyMapper, valueMapper, mergeOperator,
                () -> (M) new ConcurrentSkipListMap<K, List<V>>(keyComparator));
    }

    /**
     * Groups stream elements by mapped keys, given natural ordering on the keys and the
     * {@link List} of stream elements as values.
     *
     * @param keyMapper the {@link Function} to map a stream element as the target key
     * @return a {@link NavigableMap} with the mapped keys and the stream elements as
     * values
     */
    public static <T extends Comparable<T>, A,
            K extends Comparable<K>,
            M extends NavigableMap<K, List<T>>> Collector<T, A, M> groupAndSortBy(
            Function<? super T, ? extends K> keyMapper) {
        return groupAndSortBy(keyMapper, identity());
    }

    /**
     * Groups stream elements by mapped keys, given natural ordering on the keys and the
     * {@link List} of mapped values.
     *
     * @param keyMapper   the {@link Function} to map a stream element as the target key
     * @param valueMapper the {@link Function} to map a stream element as the target value
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    public static <T, A,
            K extends Comparable<K>,
            V extends Comparable<V>,
            M extends NavigableMap<K, List<V>>> Collector<T, A, M> groupAndSortBy(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper) {
        return groupAndSortBy(keyMapper, valueMapper, naturalOrder(), naturalOrder());
    }

    /**
     * Groups stream elements by mapped keys, given {@link Comparator} implementations for
     * both keys and values.
     *
     * @param keyMapper       the {@link Function} to map a stream element as the target key
     * @param valueMapper     the {@link Function} to map a stream element as the target value
     * @param keyComparator   the {@link Comparator} to use for the keys
     * @param valueComparator the {@link Comparator} to use for the values
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    @SuppressWarnings("unchecked")
    public static <T, A, K, V,
            M extends NavigableMap<K, List<V>>> Collector<T, A, M> groupAndSortBy(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Comparator<? super K> keyComparator,
            Comparator<? super V> valueComparator) {
        return (Collector<T, A, M>) groupingBy(keyMapper,
                () -> (M) new TreeMap<K, List<V>>(keyComparator),
                collectingAndThen(mapping(valueMapper, toList()),
                        list -> list.stream().sorted(valueComparator)
                                .collect(toList())));
    }

    /**
     * Concurrently groups stream elements by mapped keys, given natural ordering on the
     * keys and the {@link List} of stream elements as values.
     *
     * @param keyMapper the {@link Function} to map a stream element as the target key
     * @return a {@link NavigableMap} with the mapped keys and the stream elements as
     * values
     */
    public static <T extends Comparable<T>, A,
            K extends Comparable<K>,
            M extends ConcurrentNavigableMap<K, List<T>>> Collector<T, A, M> groupAndSortByConcurrent(
            Function<? super T, ? extends K> keyMapper) {
        return groupAndSortByConcurrent(keyMapper, identity());
    }

    /**
     * Concurrently groups stream elements by mapped keys, given natural ordering on the keys and the
     * {@link List} of mapped values.
     *
     * @param keyMapper   the {@link Function} to map a stream element as the target key
     * @param valueMapper the {@link Function} to map a stream element as the target value
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    public static <T, A,
            K extends Comparable<K>,
            V extends Comparable<V>,
            M extends ConcurrentNavigableMap<K, List<V>>> Collector<T, A, M> groupAndSortByConcurrent(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper) {
        return groupAndSortByConcurrent(keyMapper, valueMapper,
                naturalOrder(), naturalOrder());
    }

    /**
     * Concurrently groups stream elements by mapped keys, given {@link Comparator}
     * implementations for both keys and values.
     *
     * @param keyMapper       the {@link Function} to map a stream element as the target key
     * @param valueMapper     the {@link Function} to map a stream element as the target value
     * @param keyComparator   the {@link Comparator} to use for the keys
     * @param valueComparator the {@link Comparator} to use for the values
     * @return a {@link NavigableMap} with the mapped keys and values
     */
    @SuppressWarnings("unchecked")
    public static <T, A, K, V,
            M extends ConcurrentNavigableMap<K, List<V>>> Collector<T, A, M> groupAndSortByConcurrent(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            Comparator<? super K> keyComparator,
            Comparator<? super V> valueComparator) {
        return (Collector<T, A, M>) groupingByConcurrent(keyMapper,
                () -> (M) new ConcurrentSkipListMap<K, List<V>>(keyComparator),
                collectingAndThen(mapping(valueMapper, toList()),
                        list -> list.stream().sorted(valueComparator).collect(toList())));
    }
}
