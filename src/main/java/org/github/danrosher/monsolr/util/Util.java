package org.github.danrosher.monsolr.util;

import org.tomlj.TomlParseResult;

import java.util.Optional;
import java.util.function.Supplier;

public class Util {

    public static int getOption(TomlParseResult config, String key, Integer def){
        return Optional.ofNullable(config.getLong(key)).orElse(Long.valueOf(def)).intValue();
    }

    public static String getOption(TomlParseResult config, String key, String def){
        return Optional.ofNullable(config.getString(key)).orElse(def);
    }

    public static long getOption(TomlParseResult config, String key, Long def){
        return Optional.ofNullable(config.getLong(key)).orElse(def);
    }

    public  static String getRequired(TomlParseResult config, String key) throws Throwable {
        return getOrThrow(config,key,() -> new IllegalArgumentException("Must set "+key));
    }

    public  static String getRequired(TomlParseResult config, String key, String msg) throws Throwable {
        return getOrThrow(config,key,() -> new IllegalArgumentException(msg));
    }

    public  static String getOrThrow(TomlParseResult config, String key, Supplier<? extends Throwable> sup) throws Throwable {
        return Optional.ofNullable(config.getString(key)).orElseThrow(sup);
    }
}
