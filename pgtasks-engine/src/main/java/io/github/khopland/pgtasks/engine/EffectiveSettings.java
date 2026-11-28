package io.github.khopland.pgtasks.engine;

import java.util.Map;

/**
 * A topic's settings as the engine runs them, and where each value came from: the built-in
 * defaults, the global defaults, the module's defaults, the topic's own options, or an override.
 *
 * @param module the configurer that registered the topic
 */
public record EffectiveSettings(String topic, String module, Map<String, Setting> settings) {

    /**
     * Where a value came from. {@link #CODE} is only an engine-wide extension setting's: set on the
     * builder ({@link TaskEngineBuilder#set}).
     */
    public enum Source { DEFAULT, GLOBAL_DEFAULTS, MODULE_DEFAULTS, TOPIC, OVERRIDE, CODE }

    public record Setting(String value, Source source) {
    }

    public EffectiveSettings {
        settings = Map.copyOf(settings);
    }

    public Setting get(String key) {
        Setting setting = settings.get(key);
        if (setting == null) {
            throw new IllegalArgumentException("No setting '" + key + "' for topic " + topic + "; known: " + settings.keySet());
        }
        return setting;
    }
}
