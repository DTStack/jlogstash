#!/bin/sh

HO_HEAP_SIZE="${HO_HEAP_SIZE:=3000m}"

unset CDPATH
basedir=$(cd `dirname $0`/..; pwd)

JAVA_OPTS="$JAVA_OPTS -Xmx${HO_HEAP_SIZE}"

JAVA_OPTS="$JAVA_OPTS -Xms${HO_HEAP_SIZE}"

JAVA_OPTS="$JAVA_OPTS -server"

JAVA_OPTS="$JAVA_OPTS -Xloggc:../logs/jlogstash.gc"

JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=../logs/heapdump.hprof"

JAVA_OPTS="$JAVA_OPTS -Djava.awt.headless=true -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+HeapDumpOnOutOfMemoryError -XX:+DisableExplicitGC -Dfile.encoding=UTF-8 -Djna.nosys=true"

JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

exec java $JAVA_OPTS -cp $basedir/lib/*:$basedir/plugin/filter/*:$basedir/plugin/output/*:$basedir/plugin/input/* com.dtstack.logstash.Main "$@"