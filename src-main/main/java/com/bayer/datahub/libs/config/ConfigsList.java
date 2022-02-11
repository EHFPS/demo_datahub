package com.bayer.datahub.libs.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Immutable list of Configs.
 */
public class ConfigsList extends ArrayList<Configs> {
    private final String toString;

    public ConfigsList(List<Configs> configsList) {
        super(configsList);
        toString = stream().map(Configs::toStringWithProperties).collect(Collectors.joining());
    }

    @Override
    public String toString() {
        return toString;
    }

    @Override
    public boolean add(Configs e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Configs> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super Configs> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
}