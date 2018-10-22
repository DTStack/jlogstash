package com.google.common.collect;


import com.google.common.base.Function;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;

public class MapMakerHelper {

    public static MapMaker softValues(MapMaker mapMaker) {
        try {
            Method method = MapMaker.class.getDeclaredMethod("softValues");
            method.setAccessible(true);
            return (MapMaker) method.invoke(mapMaker);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker mapMaker, Function<? super K, ? extends V> computingFunction) {
        try {
            Method method = MapMaker.class.getDeclaredMethod("makeComputingMap",new Class[] {Function.class});
            method.setAccessible(true);
            return (ConcurrentMap<K, V>) method.invoke(mapMaker, computingFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public static void main(String[] args) {
        MapMaker mm = new MapMaker();
        MapMaker mm1 = softValues(mm);
        System.out.println(mm1);
    }
}
