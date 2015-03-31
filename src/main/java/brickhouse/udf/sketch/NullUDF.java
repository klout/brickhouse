package brickhouse.udf.sketch;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *  Return String NULL for null value.
 *   
 *  @author lance
 */
@Description(name="null",
value = "_FUNC_(x) - string or NULL. "
)
public class NullUDF extends UDF {

	public String evaluate( String str) {
		return str == null ? "NULL" : str;
	}
}
