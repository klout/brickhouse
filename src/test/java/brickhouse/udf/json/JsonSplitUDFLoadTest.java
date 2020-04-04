package brickhouse.udf.json;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.testng.annotations.Test;

/**
 * Author: Trevor DSouza (trevor.dsouza@klout.com)
 */
public class JsonSplitUDFLoadTest {

  @Test
  public void loadTest() throws HiveException {
    long startTime = System.currentTimeMillis();
    for (int i=0; i<1000000; i++) {
      String jsonArray = "{\"names\":\"John,Mary,Mike,Kate\"}";
      JsonSplitUDF jsonSplitUDF = new JsonSplitUDF();
      ObjectInspector[] objectInspectorArray = new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector};
      jsonSplitUDF.initialize(objectInspectorArray);
      Object result = jsonSplitUDF.evaluate(new DeferredObject[]{new DeferredJavaObject(jsonArray)});
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken in milliseconds: " + (endTime-startTime));
  }

}