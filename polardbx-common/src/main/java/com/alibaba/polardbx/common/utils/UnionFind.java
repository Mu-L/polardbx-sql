package com.alibaba.polardbx.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class UnionFind<T> {
    final Map<T, T> parent = new HashMap<>();
    final Map<T, Integer> rank = new HashMap<>();

    public void add(T element) {
        if (element == null){
            throw new NullPointerException();
        }
        if (!rank.containsKey(element)) {
            parent.put(element, element);
            rank.put(element, 1);
        }

    }

    public T find(T element) {
        if (element == null){
            throw new NullPointerException();
        }
        T root = element;
        while (!root.equals(parent.get(root))) {
            root = parent.get(root);
        }
        // 路径压缩
        while (!element.equals(root)) {
            T next = parent.get(element);
            parent.put(element, root);
            element = next;
        }
        return root;
    }

    public void union(T a, T b) {
        T rootA = find(a);
        T rootB = find(b);
        if (!rootA.equals(rootB)) {
            // 按秩合并
            if (rank.get(rootA) > rank.get(rootB)) {
                parent.put(rootB, rootA);
                rank.put(rootA, rank.get(rootA) + rank.get(rootB));
            } else {
                parent.put(rootA, rootB);
                rank.put(rootB, rank.get(rootB) + rank.get(rootA));
            }
        }
    }

    public boolean isConnected(T a, T b) {
        return find(a).equals(find(b));
    }

    public List<Set<T>> getAllGroup() {
        Map<T, Set<T>> map = new HashMap<>();
        for (T t : rank.keySet()) {
            T root = find(t);
            map.putIfAbsent(root, new HashSet<>());
            map.get(root).add(t);
        }
        return new ArrayList<>(map.values());
    }

    public  UnionFind<T> copy() {
        UnionFind<T> unionFind = new UnionFind<>();
        unionFind.parent.putAll(this.parent);
        unionFind.rank.putAll(this.rank);
        return unionFind;
    }
}
