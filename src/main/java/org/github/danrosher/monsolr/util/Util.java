package org.github.danrosher.monsolr.util;

import org.tomlj.TomlParseResult;

import java.util.Optional;

public class Util {

    public static int getOption(TomlParseResult config, String key, Integer def){
        return Optional.ofNullable(config.getLong(key)).orElse(Long.valueOf(def)).intValue();
    }

    public static long getOption(TomlParseResult config, String key, Long def){
        return Optional.ofNullable(config.getLong(key)).orElse(def);
    }
}
