package brickhouse.udf.collect;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

/**
 * A simple UDF that remove nulls from a list.
 */
public class ArrayRemoveNullUDF extends GenericUDF {

  private StandardListObjectInspector retValInspector;

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    List<?> list = retValInspector.getList(arg0[0].get());
    List<?> result = Lists.newArrayList();
    if (list != null && !list.isEmpty()) {
      result = Lists.newArrayList(list);
      result.removeAll(Collections.singleton(null));
    }
    return result.isEmpty() ? null : result;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "removeNulls( " + arg0[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
    if (arg0.length != 1 || arg0[0].getCategory() != Category.LIST) {
      throw new UDFArgumentException("removeNulls takes an array as argument");
    }
    retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(arg0[0]);
    return retValInspector;
  }
}
