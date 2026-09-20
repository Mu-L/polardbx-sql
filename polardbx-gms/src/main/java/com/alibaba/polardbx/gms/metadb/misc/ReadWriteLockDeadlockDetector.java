package com.alibaba.polardbx.gms.metadb.misc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReadWriteLockDeadlockDetector {

    private static final String EXCLUSIVE = "EXCLUSIVE";

    public static boolean shouldAbort(String currentOwner,
                                      Set<ReadWriteLockRecord> holders,
                                      Set<ReadWriteLockWaitingRecord> waiters) {
        if (!isDdlOwner(currentOwner) || CollectionUtils.isEmpty(waiters)) {
            return false;
        }
        Map<String, Set<String>> edges = buildWaitForGraph(holders, waiters);
        return hasCycleWhereCurrentOwnerIsVictim(currentOwner, edges);
    }

    private static Map<String, Set<String>> buildWaitForGraph(Set<ReadWriteLockRecord> holders,
                                                              Set<ReadWriteLockWaitingRecord> waiters) {
        Map<String, List<ReadWriteLockRecord>> holdersByResource = new HashMap<>();
        if (CollectionUtils.isNotEmpty(holders)) {
            for (ReadWriteLockRecord holder : holders) {
                holdersByResource.computeIfAbsent(holder.resource, ignored -> new ArrayList<>()).add(holder);
            }
        }

        Map<String, List<ReadWriteLockWaitingRecord>> waitersByResource = new HashMap<>();
        for (ReadWriteLockWaitingRecord waiter : waiters) {
            waitersByResource.computeIfAbsent(waiter.resource, ignored -> new ArrayList<>()).add(waiter);
        }
        for (List<ReadWriteLockWaitingRecord> resourceWaiters : waitersByResource.values()) {
            resourceWaiters.sort((left, right) -> Long.compare(left.queueSeq, right.queueSeq));
        }

        Map<String, Set<String>> edges = new HashMap<>();
        for (ReadWriteLockWaitingRecord waiter : waiters) {
            Set<String> blockers = findBlockers(waiter, holdersByResource.get(waiter.resource),
                waitersByResource.get(waiter.resource));
            if (!blockers.isEmpty()) {
                edges.computeIfAbsent(waiter.owner, ignored -> new HashSet<>()).addAll(blockers);
            }
        }
        return edges;
    }

    private static Set<String> findBlockers(ReadWriteLockWaitingRecord self,
                                            List<ReadWriteLockRecord> holders,
                                            List<ReadWriteLockWaitingRecord> waiters) {
        Set<String> blockers = new HashSet<>();
        boolean selfWrite = isWriteLock(self.type);
        if (CollectionUtils.isNotEmpty(holders)) {
            for (ReadWriteLockRecord holder : holders) {
                if (isSameOwner(self.owner, holder.owner)) {
                    continue;
                }
                if (isWriteLock(holder.type) || selfWrite) {
                    blockers.add(holder.owner);
                }
            }
        }
        if (CollectionUtils.isNotEmpty(waiters)) {
            for (ReadWriteLockWaitingRecord waiter : waiters) {
                if (waiter.queueSeq >= self.queueSeq) {
                    break;
                }
                if (isSameOwner(self.owner, waiter.owner)) {
                    continue;
                }
                if (selfWrite || isWriteLock(waiter.type)) {
                    blockers.add(waiter.owner);
                }
            }
        }
        return blockers;
    }

    private static boolean hasCycleWhereCurrentOwnerIsVictim(String currentOwner, Map<String, Set<String>> edges) {
        List<String> path = new ArrayList<>();
        Set<String> visiting = new HashSet<>();
        path.add(currentOwner);
        visiting.add(currentOwner);
        return dfs(currentOwner, currentOwner, edges, path, visiting);
    }

    private static boolean dfs(String currentOwner,
                               String owner,
                               Map<String, Set<String>> edges,
                               List<String> path,
                               Set<String> visiting) {
        for (String next : edges.getOrDefault(owner, new HashSet<>())) {
            if (!isDdlOwner(next)) {
                continue;
            }
            if (isSameOwner(currentOwner, next)) {
                if (isVictim(currentOwner, path)) {
                    return true;
                }
                continue;
            }
            if (visiting.contains(next)) {
                continue;
            }
            visiting.add(next);
            path.add(next);
            boolean found = dfs(currentOwner, next, edges, path, visiting);
            path.remove(path.size() - 1);
            visiting.remove(next);
            if (found) {
                return true;
            }
        }
        return false;
    }

    private static boolean isVictim(String currentOwner, List<String> cycleOwners) {
        long currentJobId = parseJobId(currentOwner);
        long maxJobId = -1L;
        for (String owner : cycleOwners) {
            maxJobId = Math.max(maxJobId, parseJobId(owner));
        }
        return currentJobId == maxJobId;
    }

    private static boolean isWriteLock(String type) {
        return EXCLUSIVE.equals(type);
    }

    private static boolean isSameOwner(String left, String right) {
        return StringUtils.equals(left, right);
    }

    private static boolean isDdlOwner(String owner) {
        return StringUtils.startsWith(owner, PersistentReadWriteLock.OWNER_PREFIX);
    }

    private static long parseJobId(String owner) {
        return Long.parseLong(owner.substring(PersistentReadWriteLock.OWNER_PREFIX.length()));
    }
}
