package brickhouse.udf.sketch;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *  Calculate the MD5 hash for a String.
 *   
 *    Useful for sketching ...
 * @author jeromebanks
 *
 */
@Description(name="java_md5",
value = "_FUNC_(x) - Hash MD5. "
)
public class JavaMD5UDF extends UDF {


	public Long evaluate( String str) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			throw new IllegalArgumentException();
		}
		byte[] buf = str.getBytes();
		byte[] md2 = md.digest(buf);
		long l = 0;
		for(int i = 0; i < 8; i++) {
		    l = l * 256;
                    l += ((long) md2[i]) & 0xff;
		}
		return l;
	}
}
