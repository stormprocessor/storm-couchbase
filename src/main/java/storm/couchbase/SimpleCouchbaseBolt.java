package storm.couchbase;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: Michael Holt
 * Date: 1/28/13
 * Time: 12:16 AM
 */
public class SimpleCouchbaseBolt extends CouchbaseBolt {

    public SimpleCouchbaseBolt(LinkedList uris, String bucket, String password) {
        super(uris, bucket, password);
    }

    @Override
    public boolean shouldActOnInput(Tuple input) {
        return true;
    }

    @Override
    public String getJSONForInput(Tuple input) {
        Gson gson = new Gson();
        HashMap<String, Object> json = new HashMap<String, Object>();

        for (String field : input.getFields()) {
            Object value = input.getValueByField(field);
            json.put(field,value);
        }
        return gson.toJson(json);
    }

    @Override
    public String getKey(Tuple input) {
        UUID key = UUID.randomUUID();
        return key.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) { }
}
