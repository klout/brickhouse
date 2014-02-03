package brickhouse.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="null_normalizer",
    value = "_FUNC_(string)"

)class NullNormalizerUDF extends UDF {
  
  public String evaluate(String input) {
    if(input == null || input.equalsIgnoreCase("null"))
      return null;
    return input;
  }
}