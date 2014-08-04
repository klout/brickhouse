package brickhouse.udf.json;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="get_main_category",
    value = "_FUNC_(string)"

)public class GetMainCategoryUDF extends UDF {
  
  public String evaluate(String input) {
    ArrayList<String> anArray = new ArrayList<String>();
    try {
      if(input == null || input.equalsIgnoreCase("null") || input.equalsIgnoreCase("[]"))
      return null;

      if(!input.startsWith("[") || !input.endsWith("]")) {
          return null;
      }

      input = input.substring(1, input.length()-1);
      
      int contInternos = 0;
      for(int i = 0; i < input.length(); i++) {
        if(input.charAt(i) == '{') {
          for(int j = i+1; j < input.length(); j++) {
            if(input.charAt(j) == '{') {
              contInternos++;
            }
            if(input.charAt(j) == '}' && contInternos == 0) {
               anArray.add("{"+input.substring(i+1, j)+"}");
               i = j;
               break;
            } else if(input.charAt(j) == '}') {
              contInternos--;
            }
          }
        }
      }

      String item;
      String model_1 = "\"parents\":[]";
      String model_2 = "\"parents\":\"null\"";
      for (int i = 0; i < anArray.size(); i++)
      {
        item = anArray.get(i);
        if(item.toLowerCase().contains(model_1.toLowerCase()) || item.toLowerCase().contains(model_2.toLowerCase()))
          return item;
      }
      return null;
    }catch (NullPointerException npe){
      return null;
    }
  }
}
