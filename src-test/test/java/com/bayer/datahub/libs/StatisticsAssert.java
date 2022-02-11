package com.bayer.datahub.libs;

import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.services.common.statistics.Statistics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class StatisticsAssert {

    public static void assertStatistics(Statistics.Type type, Statistics statistics, String title, String description,
                                        Class<? extends KafkaClient> source, Statistics.Status status,
                                        List<Statistics.Group> expGroups) {
        assertThat(statistics.getType(), equalTo(type));
        assertThat(statistics.getTitle(), equalTo(title));
        assertThat(statistics.getDescription(), equalTo(description));
        assertThat(statistics.getSource(), equalTo(source));
        assertThat(statistics.getStatus(), equalTo(status));

        var actGroups = statistics.getGroups();
        var actGroupNames = actGroups.stream().map(Statistics.Group::getName).collect(Collectors.toList());
        var expGroupNames = expGroups.stream().map(Statistics.Group::getName).toArray();
        assertThat(actGroupNames, contains(expGroupNames));
        for (int i = 0; i < actGroups.size(); i++) {
            var actGroup = actGroups.get(i);
            var expGroup = expGroups.get(i);
            var actKeys = actGroup.getValues().keySet();
            var expKeys = expGroup.getValues().keySet().toArray(new String[0]);
            assertThat(actKeys, hasItems(expKeys));
            for (String expKey : expKeys) {
                var actValue = actGroup.getValues().get(expKey);
                var expValue = expGroup.getValues().get(expKey);
                assertThat("Assert " + expKey, actValue, equalTo(expValue));
            }
        }
    }

    public static <K extends Comparable<K>, V> Map<K, V> sortedMapOf() {
        return sortedMapOf(null, null);
    }

    public static <K extends Comparable<K>, V> Map<K, V> sortedMapOf(K key1, V value1) {
        return sortedMapOf(key1, value1, null, null);
    }

    public static <K extends Comparable<K>, V> Map<K, V> sortedMapOf(K key1, V value1,
                                                                     K key2, V value2) {
        return sortedMapOf(key1, value1, key2, value2, null, null);
    }

    public static <K extends Comparable<K>, V> Map<K, V> sortedMapOf(K key1, V value1,
                                                                     K key2, V value2,
                                                                     K key3, V value3) {
        return sortedMapOf(key1, value1, key2, value2, key3, value3, null, null, null, null);
    }

    public static <K extends Comparable<K>, V> Map<K, V> sortedMapOf(K key1, V value1,
                                                                     K key2, V value2,
                                                                     K key3, V value3,
                                                                     K key4, V value4,
                                                                     K key5, V value5) {
        return sortedMapOf(
                key1, value1,
                key2, value2,
                key3, value3,
                key4, value4,
                key5, value5,
                null, null,
                null, null,
                null, null,
                null, null);
    }

    public static <K extends Comparable<K>, V> Map<K, V> sortedMapOf(K key1, V value1,
                                                                     K key2, V value2,
                                                                     K key3, V value3,
                                                                     K key4, V value4,
                                                                     K key5, V value5,
                                                                     K key6, V value6,
                                                                     K key7, V value7,
                                                                     K key8, V value8,
                                                                     K key9, V value9) {
        var map = new TreeMap<K, V>();
        if (key1 != null) {
            map.put(key1, value1);
        }
        if (key2 != null) {
            map.put(key2, value2);
        }
        if (key3 != null) {
            map.put(key3, value3);
        }
        if (key4 != null) {
            map.put(key4, value4);
        }
        if (key5 != null) {
            map.put(key5, value5);
        }
        if (key6 != null) {
            map.put(key6, value6);
        }
        if (key7 != null) {
            map.put(key7, value7);
        }
        if (key8 != null) {
            map.put(key8, value8);
        }
        if (key9 != null) {
            map.put(key9, value9);
        }
        return Collections.unmodifiableMap(map);
    }
}
