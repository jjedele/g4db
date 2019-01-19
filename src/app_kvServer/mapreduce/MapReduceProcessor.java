package app_kvServer.mapreduce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;


/**
 * Buffered map/reduce engine for aggregating data.
 */
public class MapReduceProcessor {

    private static Logger LOG = LogManager.getLogger(MapReduceProcessor.class);

    private static final String MAP_FUNCTION_NAME = "map";
    private static final String REDUCE_FUNCTION_NAME = "reduce";

    private final int bufferSize;
    private final ScriptEngine engine;
    private final Invocable invocableEngine;
    private final Map<String, List<String>> collectors;

    /**
     * Initialize the map/reduce engine with given ECMAScript5 code.
     *
     * The script must define below 2 functions:
     * map(emit, key, value) - where emit is a callback function taking a key and value, and key and value are strings
     * reduce(values) - take a list of buffered map results and reduces them to one
     *
     * @param script The script in string form
     * @throws ScriptException if the script can not be evaluated
     * @throws IllegalArgumentException if the script does not define the correct functions
     */
    public MapReduceProcessor(String script) throws ScriptException {
        bufferSize = 100;

        // create the script engine
        engine = new ScriptEngineManager().getEngineByName("nashorn");
        invocableEngine = (Invocable) engine;

        // collectors
        collectors = new HashMap<>();

        // load MR script
        engine.eval(script);

        if (null == engine.get(MAP_FUNCTION_NAME)) {
            throw new IllegalArgumentException("Map/reduce script must define a map function.");
        }
        if (null == engine.get(REDUCE_FUNCTION_NAME)) {
            throw new IllegalArgumentException("Map/reduce script must define a reduce function.");
        }
    }

    /**
     * Process one key, value entry.
     *
     * @param key Key
     * @param value Value
     */
    public void process(String key, String value) {
        try {
            invocableEngine.invokeFunction("map", mapResultConsumer, key, value);
        } catch (ScriptException | NoSuchMethodException e) {
            LOG.warn("Error processing value: " + value, e);
        }
    }

    /**
     * Get the reduced results for each encountered key.
     *
     * @return A map of key->result
     */
    public Map<String, String> getResults() {
        Map<String, String> result = new HashMap<>();

        for (String key : collectors.keySet()) {
            List<String> collector = collectors.get(key);
            if (!collector.isEmpty()) {
                reduceBuffer(key);
            }
            result.put(key, collector.get(0));
        }

        return result;
    }

    /**
     * Directly add an key,value pair to the reducer.
     *
     * This can be used to combine pre-reduced data from parallel workers.
     *
     * @param key Key of the key,value-pair.
     * @param value Value of the key,value-pair.
     */
    public void processMapResult(String key, String value) {
        List<String> keyCollector = collectors.computeIfAbsent(key, (k) -> new ArrayList<>());
        keyCollector.add(value);

        if (keyCollector.size() > bufferSize) {
            try {
                String reduced = (String) invocableEngine.invokeFunction("reduce", keyCollector);
                keyCollector.clear();
                keyCollector.add(reduced);
            } catch (ScriptException | NoSuchMethodException e) {
                LOG.warn("Error processing map result: " + value, e);
            }
        }
    }

    private void reduceBuffer(String key) {
        try {
            List<String> collector = collectors.get(key);
            assert collector != null;
            String reduced = (String) invocableEngine.invokeFunction("reduce", collector);
            collector.clear();
            collector.add(reduced);
        } catch (ScriptException | NoSuchMethodException e) {
            LOG.warn("Error reducing buffer for key " + key, e);
        }
    }

    private final BiConsumer<String, String> mapResultConsumer =
            (key, value) -> processMapResult(key, value);

}
