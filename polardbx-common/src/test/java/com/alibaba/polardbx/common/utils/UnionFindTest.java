package com.alibaba.polardbx.common.utils;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class UnionFindTest extends TestCase {

    @Test
    public void testAdd() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("A");
        uf.add("B");

        assertTrue(uf.parent.containsKey("A"));
        assertTrue(uf.rank.containsKey("A"));
        assertEquals("A", uf.parent.get("A"));
        assertEquals(1, uf.rank.get("A").intValue());

        // 测试重复添加不改变现有数据
        uf.add("A");
        assertEquals(1, uf.rank.get("A").intValue());
    }

    @Test
    public void testFindRoot() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("X");
        assertEquals("X", uf.find("X"));
    }

    @Test
    public void testUnionAndFind() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("A");
        uf.add("B");
        uf.add("C");

        uf.union("A", "B");
        assertEquals(uf.find("A"), uf.find("B"));

        uf.union("B", "C");
        assertEquals(uf.find("A"), uf.find("C"));
    }

    @Test
    public void testPathCompression() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("A");
        uf.add("B");
        uf.add("C");

        // 手动设置父节点形成链式结构
        uf.parent.put("A", "B");
        uf.parent.put("B", "C");

        uf.find("A");

        assertEquals("C", uf.parent.get("A"));
        assertEquals("C", uf.parent.get("B"));
    }

    @Test
    public void testIsConnected() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("X");
        uf.add("Y");
        uf.add("Z");

        assertFalse(uf.isConnected("X", "Y"));

        uf.union("X", "Y");
        assertTrue(uf.isConnected("X", "Y"));

        uf.union("Y", "Z");
        assertTrue(uf.isConnected("X", "Z"));
    }

    @Test
    public void testUnionByRank() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("A");
        uf.add("B");
        uf.add("C");
        uf.add("D");

        uf.union("A", "B"); // 合并A和B，假设B成为根（rank为2）
        uf.union("C", "D"); // 合并C和D，假设D成为根（rank为2）

        uf.union("B", "D"); // 合并两个rank相等的树

        // 此时其中一个会成为根，假设随机选择其中一个（测试时可能需要调整）
        // 这里假设B的rank被合并到D的根下，但具体取决于实现
        // 这里主要验证合并后的rank是否正确
        String root = uf.find("A");
        assertEquals(4, uf.rank.get(root).intValue());
    }

    @Test
    public void testGetAllGroups() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("A");
        uf.add("B");
        uf.add("C");
        uf.add("D");

        uf.union("A", "B");
        uf.union("C", "D");

        List<Set<String>> groups = uf.getAllGroup();
        assertEquals(2, groups.size());

        Set<String> groupAB = new HashSet<>(Arrays.asList("A", "B"));
        Set<String> groupCD = new HashSet<>(Arrays.asList("C", "D"));

        assertTrue(groups.contains(groupAB));
        assertTrue(groups.contains(groupCD));
    }

    @Test
    public void testComplexUnion() {
        UnionFind<String> uf = new UnionFind<>();
        uf.add("X");
        uf.add("Y");
        uf.add("Z");
        uf.add("W");

        uf.union("X", "Y");
        uf.union("Y", "Z");
        uf.union("Z", "W");

        List<Set<String>> groups = uf.getAllGroup();
        assertEquals(1, groups.size());
        Set<String> expected = new HashSet<>(Arrays.asList("X", "Y", "Z", "W"));
        assertEquals(expected, groups.get(0));
    }

    public static class Person {
        private String id;

        public Person(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return Objects.equals(id, person.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    @Test
    public void testAddPerson() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        uf.add(p1);
        assertTrue(uf.parent.containsKey(p1));
        assertEquals(p1, uf.parent.get(p1));
        assertEquals(1, uf.rank.get(p1).intValue());
    }

    @Test
    public void testFindSameIdDifferentInstances() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        Person p2 = new Person("A"); // 同一ID的不同实例
        uf.add(p1);
        uf.add(p2); // 第二次添加会被忽略（视为同一元素）

        assertEquals(p1, uf.parent.get(p1)); // 父节点是p1
        assertEquals(p1, uf.find(p2)); // find(p2)返回p1的根
    }

    @Test
    public void testUnionWithSameId() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        Person p2 = new Person("A");
        Person p3 = new Person("B");

        uf.add(p1);
        uf.add(p3);

        uf.union(p1, p3); // 合并A和B
        uf.union(p2, p3); // 使用p2（与p1相同ID）合并到B

        assertTrue(uf.isConnected(p1, p3));
        assertTrue(uf.isConnected(p2, p3)); // p2和p3也应连接
    }

    @Test
    public void testPathCompressionWithSameId() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        Person p2 = new Person("A");
        Person p3 = new Person("B");

        uf.add(p1);
        uf.add(p3);

        uf.union(p1, p3); // 合并A和B
        uf.union(p3, p2); // 合并B和p2（与p1相同ID）

        // 验证路径压缩后，所有元素的父节点指向同一根
        assertSame(uf.find(p1), uf.find(p2));
        assertSame(uf.find(p1), uf.find(p3));
    }

    @Test
    public void testGetAllGroups_WithSameId() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        Person p2 = new Person("A");
        Person p3 = new Person("B");

        uf.add(p1);
        uf.add(p2); // 被忽略
        uf.add(p3);

        uf.union(p1, p3); // 合并A和B

        List<Set<Person>> groups = uf.getAllGroup();
        assertEquals(1, groups.size());
        Set<Person> expectedGroup = new HashSet<>(Arrays.asList(p1, p3));
        assertEquals(expectedGroup, groups.get(0));
    }

    @Test
    public void testUnionUnrelatedElements() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        Person p2 = new Person("B");

        uf.add(p1);
        uf.add(p2);

        uf.union(p1, p2);
        assertTrue(uf.isConnected(p1, p2));
    }

    @Test
    public void testAddSameIdMultipleTimes() {
        UnionFind<Person> uf = new UnionFind<>();
        Person p1 = new Person("A");
        Person p2 = new Person("A");
        Person p3 = new Person("A");

        uf.add(p1);
        uf.add(p2); // 被忽略
        uf.add(p3); // 被忽略

        assertEquals(1, uf.parent.size()); // 只有一个元素存在
        assertEquals(p1, uf.parent.get(p1));
    }


}