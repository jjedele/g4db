package app_kvServer.mapreduce;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;


/**
 * Buffered map/reduce engine for aggregating data.
 */
public class MapReduceProcessor {

    private static final String MAP_FUNCTION_NAME = "map";
    private static final String REDUCE_FUNCTION_NAME = "reduce";

    private final int bufferSize;
    private final ScriptEngine engine;
    private final Invocable invocableEngine;
    private final Map<String, List<String>> collectors;

    /**
     * Initialize the map/reduce engine with given JS5 script.
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
     * @param key Key
     * @param value Value
     */
    public void process(String key, String value) {
        try {
            invocableEngine.invokeFunction("map", mapResultConsumer, key, value);
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the reduced results for each encountered key.
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

    private void processMapResult(String key, String value) {
        List<String> keyCollector = collectors.computeIfAbsent(key, (k) -> new ArrayList<>());
        keyCollector.add(value);

        if (keyCollector.size() > bufferSize) {
            try {
                String reduced = (String) invocableEngine.invokeFunction("reduce", keyCollector);
                keyCollector.clear();
                keyCollector.add(reduced);
            } catch (ScriptException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }

    private class MapResultCollector implements BiConsumer<String, String> {
        @Override
        public void accept(String key, String value) {
            processMapResult(key, value);
        }
    }

    private void reduceBuffer(String key) {
        try {
            List<String> collector = collectors.get(key);
            assert collector != null;
            String reduced = (String) invocableEngine.invokeFunction("reduce", collector);
            collector.clear();
            collector.add(reduced);
        } catch (ScriptException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private final BiConsumer<String, String> mapResultConsumer =
            (key, value) -> processMapResult(key, value);

    public static void main(String[] args) throws IOException, ScriptException {
        String script = new String(Files.readAllBytes(Paths.get("./demo_data/sales_by_country.js")));
        MapReduceProcessor mrp = new MapReduceProcessor(script);


        Stream<String> lines = Files.lines(Paths.get("./demo_data/OnlineRetail.json.txt"));
        lines.forEach(line -> {
            mrp.process("", line);
        });

        System.out.println(mrp.getResults());
    }

}
