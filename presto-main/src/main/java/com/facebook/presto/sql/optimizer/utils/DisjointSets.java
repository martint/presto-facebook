/*
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
package com.facebook.presto.sql.optimizer.utils;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

public class DisjointSets<T>
{
    private static final int INITIAL_SIZE = 10;

    private final Object2IntMap<T> indices = new Object2IntLinkedOpenHashMap<>(INITIAL_SIZE);
    private T[] nodes;
    private int[] parents;
    private int[] ranks;
    private int[] next;

    private int count;

    public DisjointSets()
    {
        indices.defaultReturnValue(-1);
        nodes = (T[]) new Object[INITIAL_SIZE];
        parents = new int[INITIAL_SIZE];
        ranks = new int[INITIAL_SIZE];
        next = new int[INITIAL_SIZE];
    }

    public void add(T element)
    {
        if (indices.containsKey(element)) {
            return;
        }

        if (count == nodes.length) {
            grow();
        }

        int index = count;
        count++;

        indices.put(element, index);
        nodes[index] = element;
        parents[index] = -1;
        next[index] = -1;
        ranks[index] = 0;
    }

    public boolean inSameSet(T element1, T element2)
    {
        int index1 = indices.getInt(element1);
        int index2 = indices.getInt(element2);

        if (index1 == -1 || index2 == -1) {
            return false;
        }

        return findRoot(index1) == findRoot(index2);
    }

    public Set<T> roots()
    {
        Set<T> result = new HashSet<T>();

        for (int i = 0; i < parents.length; i++) {
            if (parents[i] == -1) {
                result.add(nodes[i]);
            }
        }

        return result;
    }

    public T find(T element)
    {
        int index = indices.getInt(element);
        if (index == -1) {
            return null;
        }

        return nodes[findRoot(index)];
    }

    public Iterable<T> findAll(T element)
    {
        int index = indices.getInt(element);
        if (index == -1) {
            return ImmutableList.of();
        }

        return () -> new AbstractIterator<T>()
        {
            private int current = findRoot(index);

            @Override
            protected T computeNext()
            {
                if (current == -1) {
                    return endOfData();
                }

                T result = nodes[current];
                current = next[current];
                return result;
            }
        };
    }

    public void union(T a, T b)
    {
        int aIndex = indices.getInt(a);
        int bIndex = indices.getInt(b);

        checkArgument(aIndex != -1, "Element not found: %s", a);
        checkArgument(bIndex != -1, "Element not found: %s", b);

        int aRoot = findRoot(aIndex);
        int bRoot = findRoot(bIndex);

        if (aRoot == bRoot) {
            return;
        }

        if (ranks[aRoot] < ranks[bRoot]) {
            parents[aRoot] = bRoot;
            next[findLast(bRoot)] = aRoot;
        }
        else if (ranks[aRoot] > ranks[bRoot]) {
            parents[bRoot] = aRoot;
            next[findLast(aRoot)] = bRoot;
        }
        else {
            parents[bRoot] = aRoot;
            next[findLast(aRoot)] = bRoot;
            ranks[aRoot]++;
        }
    }

    public Collection<Set<T>> sets()
    {
        Map<Integer, Set<T>> sets = new HashMap<>();
        for (int i = 0; i < count; i++) {
            int root = findRoot(i);
            sets.computeIfAbsent(root, v -> new HashSet<>()).add(nodes[i]);
        }
        return sets.values();
    }

    public int size()
    {
        return count;
    }

    private int findRoot(int index)
    {
        int parent = parents[index];
        if (parent == -1) {
            return index;
        }

        int root = findRoot(parent);

        // path compression
        parents[index] = root;

        return root;
    }

    private int findLast(int index)
    {
        int current = index;
        int child = next[current];
        while (child != -1) {
            current = child;
            child = next[current];
        }

        return current;
    }

    private void grow()
    {
        int newCapacity = nodes.length * 2;
        nodes = Arrays.copyOf(nodes, newCapacity);
        parents = Arrays.copyOf(parents, newCapacity);
        ranks = Arrays.copyOf(ranks, newCapacity);
        next = Arrays.copyOf(next, newCapacity);
    }

    public static void main(String[] args)
    {
        DisjointSets<Integer> set = new DisjointSets<>();

        for (int i = 0; i < 10; i++) {
            set.add(i);
        }

        set.union(0, 1);
        set.union(2, 3);
        set.union(4, 5);
        set.union(6, 7);
        set.union(8, 9);

        set.union(2, 4);
        set.union(0, 2);

        for (int i = 0; i < 10; i++) {
            System.out.println(i + " -> " + set.find(i) + " " + ImmutableList.copyOf(set.findAll(i)));
        }

        System.out.println();
    }
}
