package brickhouse.udf.collect;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

/**
 * Creates a session id for an index and a time stamp. Default session length is 30 minute = 1800000 milliseconds
 */
import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
		name="sessionize_edmodo", 
		value="_FUNC_(string, timestamp) - Returns a json tuple session id for the given id and ts(long). Optional third parameter to specify interval tolerance in milliseconds",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo; {session_id:,session_time:,session_order:")

public class SessionizeEdmodoUDF extends UDF {
		private String lastUid = null;
		private long order = -1;
		private long origTS = -1;
		private long lastTS = -1;
		private String lastUUID = null;

	  
	  public String evaluate(String uid, long ts, int tolerance) {
		  if (uid.equals(lastUid) && timeStampCompare(lastTS, ts, tolerance)) { 
			  lastTS = ts;		
			  order++;
		  } else if (uid.equals(lastUid)) {
			  lastTS = ts;
			  origTS = ts;
			  lastUUID=UUID.randomUUID().toString();
			  order = 1;
		  } else {
			  lastUid = uid;
			  lastTS = ts;
			  origTS = ts;
			  lastUUID=UUID.randomUUID().toString();
			  order = 1;
		  }
		  StringBuilder sb = new StringBuilder();
		  sb.append("{\"session_id\":\"");
		  sb.append(lastUid);
		  sb.append("\",\"session_time\":");
		  sb.append(Long.toString(origTS));
		  sb.append(",\"session_order\":");
		  sb.append(Long.toString(order));
		  sb.append("}");
		  return lastUUID;
	  }
	  public String evaluate(String uid,  long ts) { 
		 return evaluate(uid, ts, 1800000);
	  }
	  
	  private Boolean timeStampCompare(long lastTS, long ts, int ms) { 
		  try {
			  long difference = ts - lastTS;
			  return (Math.abs((int)difference) < ms) ? true : false;
		  } catch (ArithmeticException e) {
			  return false;
		  }
	  }
	}