package com.google.common.collect;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;

public class MigrateMap {

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker maker, Function<? super K, ? extends V> computingFunction) {
        return MapMakerHelper.makeComputingMap(maker, computingFunction);
    }

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
        return MapMakerHelper.makeComputingMap(new MapMaker(), computingFunction);
    }

}