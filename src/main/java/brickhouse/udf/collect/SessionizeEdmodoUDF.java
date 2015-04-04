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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
		name="sessionize_edmodo", 
		value="_FUNC_(string, timestamp) - Returns a json tuple session id for the given session id and ts(long). Optional third parameter to specify interval tolerance in milliseconds",
		extended="SELECT _FUNC_(uid, ts), uid, ts, event_type from foo; {session_id:,session_time:,session_order:}")

public class SessionizeEdmodoUDF extends UDF {
		private Long lastSessionId = null;
		private long order = -1;
		private long origTS = -1;
		private long lastTS = -1;

	  
	  public String evaluate(Long uid, long ts, int tolerance) {
		  if (uid.equals(lastSessionId) && timeStampCompare(lastTS, ts, tolerance)) { 
			  lastTS = ts;	
			  order++;
		  } else if (uid.equals(lastSessionId)) {
			  lastTS = ts;
			  origTS = ts;
			  order = 1;
		  } else {
			  lastSessionId = uid;
			  lastTS = ts;
			  origTS = ts;
			  order = 1;
		  }
		  StringBuilder sb = new StringBuilder();
		  sb.append("{\"session_id\":\"");
		  sb.append(lastSessionId);
		  sb.append("\",\"session_start\":\"");
		  sb.append(Long.toString(origTS));
		  sb.append("\",\"session_end\":\"");
		  sb.append(Long.toString(lastTS));
		  sb.append("\",\"session_order\":\"");
		  sb.append(Long.toString(order));
		  sb.append("\"}");
		  return sb.toString();
	  }
	  
	  public String evaluate(Long session_id,  long ts) { 
		 return evaluate(session_id, ts, 1800000);
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
