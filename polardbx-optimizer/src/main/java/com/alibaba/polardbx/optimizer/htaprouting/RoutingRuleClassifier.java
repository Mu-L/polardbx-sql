package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class RoutingRuleClassifier {
    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingRuleManager.class);

    private final Map<String, UserSpecificRoutingRuleClassifier> userSpecificMap;

    private final ImmutableMap<Long, LongAdder> hitCounterMap;

    private final Cache<RoutingCacheKey, Optional<CountedRoutingType>> keywordRoutingCache;

    private boolean hasFollowerRead;

    private RoutingRuleClassifier(Map<String, UserSpecificRoutingRuleClassifier> userMap,
                                  ImmutableMap<Long, LongAdder> hitCounterMap) {
        this.userSpecificMap = userMap;
        this.hitCounterMap = hitCounterMap;
        this.keywordRoutingCache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(20000)
            .expireAfterWrite(12 * 3600 * 1000, TimeUnit.MILLISECONDS)
            .build();
        this.hasFollowerRead = false;
        for (UserSpecificRoutingRuleClassifier classifier : userMap.values()) {
            if (classifier.classifyByUserName() != null && classifier.classifyByUserName().isFollowerRouting()) {
                this.hasFollowerRead = true;
                break;
            }
        }
    }

    public static class UserSpecificRoutingRuleClassifier {
        final ImmutableMap<String, CountedRoutingType> templateIdTypeMap;
        final ImmutableList<Pair<List<String>, CountedRoutingType>> keyWordsTypePair;
        final CountedRoutingType userRouting;

        public UserSpecificRoutingRuleClassifier(ImmutableMap<String, CountedRoutingType> templateIdMap,
                                                 ImmutableList<Pair<List<String>, CountedRoutingType>> keyWordsMap,
                                                 CountedRoutingType userRouting) {
            this.templateIdTypeMap = templateIdMap;
            this.keyWordsTypePair = keyWordsMap;
            this.userRouting = userRouting;
        }

        CountedRoutingType classifyByTemplateId(String templateId) {
            return templateIdTypeMap.get(templateId);
        }

        CountedRoutingType classifyByUserName() {
            return userRouting;
        }

        boolean isKeywordEmpty() {
            return CollectionUtils.isEmpty(keyWordsTypePair);
        }

        CountedRoutingType classifyByKeywords(ExecutionContext ec, String sql) {
            CountedRoutingType countedRoutingType = null;
            int[] indexList = new int[keyWordsTypePair.size()];
            MySqlLexer lexer = new MySqlLexer(ByteString.from(sql));
            do {
                lexer.nextToken();
                String word =
                    lexer.subString(lexer.getStartPos(), lexer.pos() - lexer.getStartPos()).toLowerCase().trim();
                if (StringUtils.isEmpty(word)) {
                    continue;
                }
                word = SQLUtils.normalizeNoTrim(word);
                for (int i = 0; i < indexList.length; i++) {
                    List<String> keywords = keyWordsTypePair.get(i).getKey();
                    if (indexList[i] < keywords.size() && word.equals(
                        keywords.get(indexList[i]))) {
                        if ((++indexList[i]) == keywords.size()) {
                            CountedRoutingType tmpCountedRoutingType = CountedRoutingType.getHigherRoutingType(
                                countedRoutingType, keyWordsTypePair.get(i).getValue());
                            if (countedRoutingType != tmpCountedRoutingType) {
                                countedRoutingType = tmpCountedRoutingType;
                                HtapTrace.addTrace(ec.getHtapTrace(),
                                    String.format("find routing type: %s, hit by keywords {%s}",
                                        tmpCountedRoutingType.getRoutingType().name(),
                                        String.join(", ", keywords)));
                            }
                        }
                    }
                }
            } while (lexer.token() != Token.EOF);
            return countedRoutingType;
        }
    }

    private static class RoutingCacheKey {
        String templateId;
        String userName;

        public RoutingCacheKey(String templateId, String userName) {
            this.templateId = templateId;
            this.userName = userName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RoutingCacheKey cacheKey = (RoutingCacheKey) o;
            return templateId.equals(cacheKey.templateId)
                && userName.equals(cacheKey.userName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(templateId, userName);
        }
    }

    public Callable<Optional<CountedRoutingType>> getValueLoader(ExecutionContext ec,
                                                                 UserSpecificRoutingRuleClassifier userClassifier,
                                                                 UserSpecificRoutingRuleClassifier allClassifier,
                                                                 String sql) {
        return () -> {
            if (StringUtils.isEmpty(sql)) {
                return Optional.empty();
            }
            CountedRoutingType countedRoutingType = null;
            if (userClassifier != null && !userClassifier.isKeywordEmpty()) {
                countedRoutingType = userClassifier.classifyByKeywords(ec, sql);
            }
            if (countedRoutingType == null && allClassifier != null && !allClassifier.isKeywordEmpty()) {
                countedRoutingType = allClassifier.classifyByKeywords(ec, sql);
            }
            return Optional.ofNullable(countedRoutingType);
        };
    }

    public RoutingType classify(ExecutionContext ec, String templateId, String sql) {
        String currentUser = ec.getParamManager().getString(ConnectionParams.MOCK_ROUTING_USER);
        if (StringUtils.isEmpty(currentUser)) {
            if (ec.getPrivilegeContext() != null) {
                currentUser = ec.getPrivilegeContext().getUser();
            }
        }
        if (currentUser == null) {
            return null;
        }
        HtapTrace.addTrace(ec.getHtapTrace(), "current user: " + currentUser);
        UserSpecificRoutingRuleClassifier userClassifier = userSpecificMap.get(currentUser);
        UserSpecificRoutingRuleClassifier allClassifier = userSpecificMap.get(RoutingRuleManager.ALL_USER);
        CountedRoutingType countedRoutingType = null;

        // routing by user if follower read exists
        if (userClassifier != null) {
            CountedRoutingType userRouting = userClassifier.classifyByUserName();
            if (userRouting != null && userRouting.isFollowerRouting()) {
                HtapTrace.traceRoutingType(ec.getHtapTrace(), userRouting,
                    "determined by user:" + currentUser);
                userRouting.add();
                FollowerReadRecord.getInstance().access();
                return userRouting.getRoutingType();
            }
        }

        // routing by template id if without hint
        if (!StringUtils.isEmpty(templateId)) {
            if (userClassifier != null) {
                countedRoutingType = CountedRoutingType.getHigherRoutingType(countedRoutingType,
                    userClassifier.classifyByTemplateId(templateId));
            }
            if (countedRoutingType == null && allClassifier != null) {
                countedRoutingType = CountedRoutingType.getHigherRoutingType(countedRoutingType,
                    allClassifier.classifyByTemplateId(templateId));
            }
        }
        if (countedRoutingType != null) {
            HtapTrace.traceRoutingType(ec.getHtapTrace(), countedRoutingType,
                "determined by template id: " + templateId);
            countedRoutingType.add();
            return countedRoutingType.getRoutingType();
        }

        // routing by keywords
        if (((userClassifier != null && !userClassifier.isKeywordEmpty()) ||
            (allClassifier != null && !allClassifier.isKeywordEmpty()))) {
            try {
                if (!StringUtils.isEmpty(templateId)) {
                    RoutingCacheKey cacheKey = new RoutingCacheKey(currentUser, templateId);
                    countedRoutingType = keywordRoutingCache.get(cacheKey,
                        getValueLoader(ec, userClassifier, allClassifier, sql)).orElse(null);
                    if (countedRoutingType != null) {
                        HtapTrace.traceRoutingType(ec.getHtapTrace(), countedRoutingType,
                            "determined by keywords from cache");
                        countedRoutingType.add();
                        return countedRoutingType.getRoutingType();
                    }
                } else {
                    countedRoutingType = getValueLoader(ec, userClassifier, allClassifier, sql).call().orElse(null);
                    if (countedRoutingType != null) {
                        HtapTrace.traceRoutingType(ec.getHtapTrace(), countedRoutingType, "determined by keywords");
                        countedRoutingType.add();
                        return countedRoutingType.getRoutingType();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // routing by user
        if (userClassifier != null) {
            countedRoutingType =
                CountedRoutingType.getHigherRoutingType(countedRoutingType, userClassifier.classifyByUserName());
            if (countedRoutingType != null) {
                HtapTrace.traceRoutingType(ec.getHtapTrace(), countedRoutingType,
                    "determined by user:" + currentUser);
                countedRoutingType.add();
                return countedRoutingType.getRoutingType();
            }
        }

        HtapTrace.addTrace(ec.getHtapTrace(), "no routing rule found");
        return null;
    }

    public Map<Long, Long> getIdCounterMap() {
        if (hitCounterMap == null) {
            return ImmutableMap.of();
        }
        ImmutableMap.Builder<Long, Long> idCounterMapBuilder = ImmutableMap.builder();
        for (Map.Entry<Long, LongAdder> entry : hitCounterMap.entrySet()) {
            idCounterMapBuilder.put(entry.getKey(), entry.getValue().longValue());
        }
        return idCounterMapBuilder.build();
    }

    public boolean isHasFollowerRead() {
        return hasFollowerRead;
    }

    public static RoutingRuleClassifier build(List<RoutingRuleRecord> allRecords) {
        ImmutableMap.Builder<Long, LongAdder> hitCounterMapBuilder = ImmutableMap.builder();
        if (CollectionUtils.isEmpty(allRecords)) {
            return new RoutingRuleClassifier(Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER),
                hitCounterMapBuilder.build());
        }
        try {
            allRecords.sort((x, y) -> x.userName.compareToIgnoreCase(y.userName));
            Map<String, UserSpecificRoutingRuleClassifier> classifierMap =
                Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

            Map<String, CountedRoutingType> templateIdMap = Maps.newHashMap();
            ImmutableList.Builder<Pair<List<String>, CountedRoutingType>> keyWordsListBuilder = ImmutableList.builder();
            CountedRoutingType userRouting = null;
            String currentUser = null;
            for (RoutingRuleRecord record : allRecords) {
                if (!StringUtils.equalsIgnoreCase(currentUser, record.userName)) {
                    if (currentUser != null) {
                        classifierMap.put(currentUser,
                            new UserSpecificRoutingRuleClassifier(
                                ImmutableMap.copyOf(templateIdMap), keyWordsListBuilder.build(), userRouting));
                    }
                    templateIdMap = Maps.newHashMap();
                    keyWordsListBuilder = ImmutableList.builder();
                    userRouting = null;
                    currentUser = record.userName;
                }
                LongAdder hitCounter = new LongAdder();
                if (RoutingType.getType(record.routingType) == null) {
                    continue;
                }
                CountedRoutingType countedRoutingType = new CountedRoutingType(hitCounter,
                    RoutingType.getType(record.routingType));

                hitCounterMapBuilder.put(record.id, hitCounter);
                if (!StringUtils.isEmpty(record.templateId)) {
                    countedRoutingType = CountedRoutingType.getHigherRoutingType(templateIdMap.get(record.templateId),
                        countedRoutingType);
                    templateIdMap.put(record.templateId, countedRoutingType);
                    continue;
                }
                if (!CollectionUtils.isEmpty(record.keywords)) {
                    keyWordsListBuilder.add(Pair.of(record.keywords, countedRoutingType));
                    continue;
                }
                userRouting = CountedRoutingType.getHigherRoutingType(userRouting, countedRoutingType);
            }

            if (currentUser != null) {
                classifierMap.put(currentUser,
                    new UserSpecificRoutingRuleClassifier(
                        ImmutableMap.copyOf(templateIdMap), keyWordsListBuilder.build(), userRouting));
            }
            LOGGER.warn("finish reload " + allRecords.size() + " routing rules");
            return new RoutingRuleClassifier(classifierMap, hitCounterMapBuilder.build());
        } catch (Exception e) {
            LOGGER.error("reload routing rules failed", e);
            return new RoutingRuleClassifier(Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER), ImmutableMap.of());
        }
    }
}
