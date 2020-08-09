/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.kudu;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.config.ConfigDef;

public class ConfigKeys implements Iterable<ConfigKeys.Key> {

    private final Map<String, Key> keysByName = new LinkedHashMap<>();
    private final Map<String, AtomicInteger> orderInGroupMap = new HashMap<>();

    public ConfigKeys() {
    }

    public ConfigKeys(ConfigDef configDef) {
        Objects.requireNonNull(configDef);
        configDef.configKeys().values().forEach((configKey) -> {
            this.define(configKey.name, configKey.type)
                    .defaultValue(configKey.defaultValue)
                    .group(configKey.group)
                    .width(configKey.width)
                    .validator(configKey.validator)
                    .importance(configKey.importance)
                    .documentation(configKey.documentation)
                    .orderInGroup(configKey.orderInGroup)
                    .displayName(configKey.displayName)
                    .dependents(configKey.dependents)
                    .recommender(configKey.recommender)
                    .internal(configKey.internalConfig);
        });
    }

    public synchronized ConfigKeys.Key define(String name) {
        requireNotBlank(name);
        return this.keysByName.computeIfAbsent(name, Key::new);
    }

    public synchronized ConfigKeys.Key define(String name, ConfigDef.Type type) {
        requireNotBlank(name);
        return this.define(name).type(type);
    }

    public synchronized ConfigKeys.Key getOrDefine(ConfigKeys.Key key) {
        if (key == null) {
            return null;
        }

        Key existedKey = this.get(key.name());
        if (existedKey == null) {
            return this.define(key.name()).copy(key);
        }

        return existedKey.copy(key);
    }

    public synchronized ConfigKeys.Key get(String name) {
        Objects.requireNonNull(name);
        return this.keysByName.get(name);
    }

    public synchronized boolean contains(String name) {
        Objects.requireNonNull(name);
        return this.keysByName.containsKey(name);
    }

    public synchronized boolean remove(String name) {
        Objects.requireNonNull(name);
        return this.keysByName.remove(name) != null;
    }

    @Override
    public Iterator<ConfigKeys.Key> iterator() {
        return this.keysByName.values().iterator();
    }

    @Override
    public Spliterator<Key> spliterator() {
        return this.keysByName.values().spliterator();
    }

    public ConfigDef toConfigDef() {
        ConfigDef configDef = new ConfigDef();
        this.keysByName.values().forEach((key) -> {
            configDef.define(key.buildConfigKey());
        });

        return configDef;
    }

    private void requireNotBlank(String name) {
        if (name == null || name.trim().length() == 0) {
            throw new IllegalArgumentException("The name may not be composed of only whitespace");
        }
    }

    public class Key {
        private final String name;
        private String group;
        private int orderInGroup;
        private String displayName;
        private String documentation;
        private boolean internalConfig;
        private ConfigDef.Validator validator;
        private ConfigDef.Recommender recommender;
        private ConfigDef.Type type = ConfigDef.Type.STRING;
        private ConfigDef.Width width = ConfigDef.Width.NONE;
        private final List<String> dependents = new ArrayList<>();
        private Object defaultValue = ConfigDef.NO_DEFAULT_VALUE;
        private ConfigDef.Importance importance = ConfigDef.Importance.HIGH;

        Key(String name) {
            requireNotBlank(name);
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        public ConfigDef.Type type() {
            return this.type;
        }

        public ConfigKeys.Key type(ConfigDef.Type type) {
            Objects.requireNonNull(type);
            this.type = type;
            return this;
        }

        public String group() {
            return this.group;
        }

        public ConfigKeys.Key group(String group) {
            this.group = group;
            if (this.orderInGroup <= 0) {
                AtomicInteger orderInGroup = orderInGroupMap.computeIfAbsent(group, (key) -> new AtomicInteger(0));
                orderInGroup(orderInGroup.incrementAndGet());
            }

            return this;
        }

        public int orderInGroup() {
            return this.orderInGroup;
        }

        public ConfigKeys.Key orderInGroup(int orderInGroup) {
            this.orderInGroup = orderInGroup;
            return this;
        }

        public String displayName() {
            return this.displayName != null ? this.displayName : this.name();
        }

        public ConfigKeys.Key displayName(String displayName) {
            requireNotBlank(displayName);
            this.displayName = displayName.trim();
            return this;
        }

        public String documentation() {
            return this.documentation;
        }

        public ConfigKeys.Key documentation(String doc) {
            requireNotBlank(doc);
            this.documentation = doc.trim();
            return this;
        }

        public boolean isInternal() {
            return this.internalConfig;
        }

        public ConfigKeys.Key internal(boolean internal) {
            this.internalConfig = internal;
            return this;
        }

        public ConfigDef.Validator validator() {
            return this.validator;
        }

        public ConfigKeys.Key validator(ConfigDef.Validator validator) {
            this.validator = validator;
            return this;
        }

        public ConfigDef.Recommender recommender() {
            return this.recommender;
        }

        public ConfigKeys.Key recommender(ConfigDef.Recommender recommender) {
            this.recommender = recommender;
            return this;
        }

        public ConfigDef.Width width() {
            return this.width;
        }

        public ConfigKeys.Key width(ConfigDef.Width width) {
            Objects.requireNonNull(width);
            this.width = width;
            return this;
        }

        public List<String> dependents() {
            return this.dependents;
        }

        public ConfigKeys.Key dependents(String dependent) {
            requireNotBlank(dependent);
            this.dependents.add(dependent);
            return this;
        }

        public ConfigKeys.Key dependents(String... dependentNames) {
            return dependents(Arrays.asList(dependentNames));
        }

        public ConfigKeys.Key dependents(List<String> dependentNames) {
            for (String dependent : dependentNames) {
                dependents(dependent);
            }

            return this;
        }

        public Object defaultValue() {
            return this.defaultValue;
        }

        public ConfigKeys.Key defaultValue(Object value) {
            this.defaultValue = value;
            return this;
        }

        public ConfigDef.Importance importance() {
            return this.importance;
        }

        public ConfigKeys.Key importance(ConfigDef.Importance importance) {
            Objects.requireNonNull(importance);
            this.importance = importance;
            return this;
        }

        public ConfigKeys.Key copy(ConfigKeys.Key other) {
            this.type(other.type()).defaultValue(other.defaultValue()).validator(other.validator()).importance(other.importance()).documentation(other.documentation())
                    .group(other.group()).orderInGroup(other.orderInGroup()).width(other.width()).displayName(other.displayName).dependents(other.dependents())
                    .recommender(other.recommender()).internal(other.isInternal());
            return this;
        }

        public ConfigDef.ConfigKey buildConfigKey() {
            return new ConfigDef.ConfigKey(this.name(), this.type(), this.defaultValue(), this.validator(), this.importance(), this.documentation(), this.group(),
                    this.orderInGroup(), this.width(), this.displayName(), this.dependents(), this.recommender(), this.isInternal());
        }
    }
}
