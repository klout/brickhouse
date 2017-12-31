package ru.mail.mining.mining_dwh.common.hive.udf.json;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "to_camel_case",
        value = "_FUNC_(a) - Converts a string containing underscores to CamelCase"
)
public class ConvertToCamelCaseUDF extends UDF {

    public String evaluate(String underscore) {
        return FromJsonUDF.ToCamelCase(underscore);
    }
}
