package storm.couchbase;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * Author: Michael Holt <ilion @ sin-inc.net>
 * Date: 1/28/13
 * Time: 12:16 AM
 */
public class SimpleCouchbaseBolt extends CouchbaseBolt {

    public SimpleCouchbaseBolt(LinkedList<URI> uris, String bucket, String password) {
        super(uris, bucket, password);
    }

    /**
     * Validates a tuple
     *
     * Should validate the tuple and decide if it should be added to the couchBase store.
     * Here we're just going to return true always.
     *
     * @param input A backtype.storm.tuple.Tuple
     * @return true
     */
    @Override
    public boolean shouldActOnInput(Tuple input) {
        return true;
    }

    /**
     * Transforms a Tuple into valid json
     *
     * Iterates through each field in the tuple to create json fields and
     * assigns the values as json values. Assumes a single dimension,
     *
     * @param input A backtype.storm.tuple.Tuple
     * @return A Json string
     */
    @Override
    public String getJSONForInput(Tuple input) {
        Gson gson = new Gson();
        HashMap<String, String> json = new HashMap<String, String>();

        for (String field : input.getFields()) {
            Object value = input.getValueByField(field);
            json.put(field,value.toString());
        }
        return gson.toJson(json);
    }

    /**
     * Creates a unique value for inserting into couchBase
     *
     * @param input A backtype.storm.tuple.Tuple
     * @return A UUID
     */
    @Override
    public String getKey(Tuple input) {
        UUID key = UUID.randomUUID();
        return key.toString();
    }

    /**
     * Declares output fields for Storm topology
     *
     * We're not bothering to output anything for this sample.
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // we aren't bothering to output any more tuples
    }
}
