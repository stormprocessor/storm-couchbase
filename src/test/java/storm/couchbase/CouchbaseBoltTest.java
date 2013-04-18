package storm.couchbase;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import static org.junit.Assert.*;
import org.junit.*;

import java.net.URI;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * Author: Michael Holt <ilion @ sin-inc.net>
 * Date: 1/25/13
 * Time: 10:26 PM
 */
public class CouchbaseBoltTest  {
    LinkedList <URI> uris = new LinkedList<URI>();
    String fakeBucket = "fake";
    String fakePassword = "pass";

    private CouchbaseBolt testCouchBolt = new CouchbaseBolt(uris, fakeBucket, fakePassword) {
        @Override
        public boolean shouldActOnInput(Tuple input) {
            return false;
        }

        @Override
        public String getJSONForInput(Tuple input) {
            return null;
        }

        @Override
        public String getKey(Tuple input) {
            return null;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // No point in declaring this right now
        }
    };

    @Before
    public void testSetup() {

    }

    @Test
    public void getExpireTimeTest() {
        assertTrue(testCouchBolt.getExpireTime() == 0);
    }

    @Test
    public void setExpireTimeTest() {
        testCouchBolt.setExpireTime(5000);
        assertTrue(testCouchBolt.getExpireTime() == 5000);
    }

}
