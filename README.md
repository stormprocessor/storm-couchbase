storm-couchbase
===============

Allows the creation of Couchbase Bolts in Storm. 

storm.couchbase.CouchbaseBolt provides an abstract class which handles the Couchbase connections, excution, failing and acking. You need to provide a method for turning a tuple into valid json and creating a key for the Couchbase record.

storm.couchbase.SimpleCouchbaseBolt serves as an example implementation. It is not expected to be used in a production environment as you would want proper tuple validation and useful keys. It is intended as an example to get you going.

getJsonForInput
---------------

The meat of this bolt will happen in the getJsonForInput function which must be overwritten. The simpletest version, used in the SimpleCouchbaseBolt simply takes the tuple and turns it into json:

```java
public String getJSONForInput(Tuple input) {
  Gson gson = new Gson();
  HashMap<String, String> json = new HashMap<String, String>();

  for (String field : input.getFields()) {
    Object value = input.getValueByField(field);
    json.put(field,value.toString());
  }
  return gson.toJson(json);
}
```

This iterates through the tuple, finding each field key and retrieving the corresponding value. It expects only a flat json object. A complete version would look for nested json values. This should be easily accomplished simply by running the value through gson.toJson() as well.

Couchbase
=========

For everything Couchbase see http://www.couchbase.com
