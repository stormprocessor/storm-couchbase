package storm.couchbase;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;


import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * Author: Michael Holt <ilion @ sin-inc.net>
 * Date: 17/04/13
 * Time: 9:11 PM
 */


public class SimpleCouchbaseBoltTest {
    private LinkedList<URI> uris = new LinkedList<URI>();
    private URI uri;
    Tuple testTuple;

    private SimpleCouchbaseBolt simpleCouchbaseTestBolt;

    @Test
    public void testShouldActOnInput() throws Exception {

        assertTrue(simpleCouchbaseTestBolt.shouldActOnInput(testTuple));

    }

    @Before
    public void testSetup() {

        LinkedList<String> fieldList = new LinkedList<String>();
        fieldList.add("firstKey");
        fieldList.add("secondKey");
        fieldList.add("thirdKey");
        Fields fields = new Fields(fieldList);

        testTuple = mock(Tuple.class);
        when(testTuple.getFields()).thenReturn(fields);
        when(testTuple.getValueByField("firstKey")).thenReturn("firstValue");
        when(testTuple.getValueByField("secondKey")).thenReturn("secondValue");
        when(testTuple.getValueByField("thirdKey")).thenReturn("thirdValue");

        try {
            uri = new URI("http://localhost/");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        uris.add(uri);

        simpleCouchbaseTestBolt = new SimpleCouchbaseBolt(uris,"test","test");


    }
    @Test
    public void testGetJSONForInput() throws Exception {
        String testString = "{\"firstKey\":\"firstValue\",\"secondKey\":\"secondValue\",\"thirdKey\":\"thirdValue\"}";
        String jsonString = simpleCouchbaseTestBolt.getJSONForInput(testTuple);

        assertEquals("Json formed correctly", jsonString, testString);

    }

    @Test
    public void testGetKey() throws Exception {
        String key = simpleCouchbaseTestBolt.getKey(testTuple);

        assertTrue(key.matches("[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"));

    }

    @Test
    public void testDeclareOutputFields() throws Exception {
        //nothing to test

    }
}
