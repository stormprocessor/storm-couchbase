package storm.couchbase;

/**
 * Created with IntelliJ IDEA.
 * Author: Michael Holt <ilion @ sin-inc.net>
 * Date: 1/25/13
 * Time: 10:23 PM
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class CouchbaseBolt extends BaseRichBolt {

    private OutputCollector collector;
    private CouchbaseClient client;
    private int expireTime;

    private final List<URI> uris = new LinkedList<URI>();
    private final String bucket;
    private final String password;

    protected CouchbaseBolt(LinkedList<URI> uris, String bucket, String password) throws RuntimeException {
        try
        {
            this.uris.addAll(uris);
        }
        catch (Exception c)
        {
            throw new RuntimeException("Failed to register Couchbase URIs.", c.getCause());
        }

        this.bucket = bucket;
        this.password = password;
        this.expireTime = 0;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
        try
        {
            this.client = new CouchbaseClient(this.uris, this.bucket, this.password);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input)
    {
        if (shouldActOnInput(input))
        {
            String key = getKey(input);
            String jsonInput = getJSONForInput(input);
            OperationFuture<Boolean> setOp = client.set(key, this.expireTime, jsonInput);
            try {
                if (setOp.get())
                {
                    this.collector.ack(input);
                }
                else
                {
                    this.collector.fail(input);
                }
            } catch (InterruptedException e) {
                this.collector.fail(input);
            } catch (ExecutionException e) {
                this.collector.fail(input);
            }
        }
        else
        {
            this.collector.ack(input);
        }
    }

    @Override
    public void cleanup() {
        this.client.shutdown(5, TimeUnit.MINUTES);
    }

    public abstract boolean shouldActOnInput(Tuple input);

    public abstract String getJSONForInput(Tuple input);

    public abstract String getKey(Tuple input);

    public int getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }
}
