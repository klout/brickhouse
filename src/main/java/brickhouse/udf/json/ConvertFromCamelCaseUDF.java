package brickhouse.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "from_camel_case",
        value = "_FUNC_(a) - Converts a string in CamelCase to one containing underscores."
)
public class ConvertFromCamelCaseUDF extends UDF {

    public String evaluate(String camel) {
        return FromJsonUDF.FromCamelCase(camel);
    }
}
