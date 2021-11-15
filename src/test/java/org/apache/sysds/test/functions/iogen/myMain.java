package org.apache.sysds.test.functions.iogen;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;

import java.util.Set;

public class myMain {

	public static void main(String[] args) throws JSONException {
		//{"id":123,"name":"saeed","address":["inf13", "Artur-Michl-Gasse 8"],"family":[{"id":456,"name":"narges"},{"id":789,"name":"keysan"}]}
		String js="{\"id\":123,\"name\":\"saeed\",\"address\":[\"inf13\", \"Artur-Michl-Gasse 8\"],\"family\":[{\"id\":456,\"name\":\"narges\"},{\"id\":789,\"name\":\"keysan\"}]}";
		JSONObject jsonObject=new JSONObject(js);
		JSONArray jsonArray = jsonObject.getJSONArray("family");

		if(jsonArray.get(1)!=null)

			System.out.println("sssssssssss");


//
//		Set<Integer> set1 =Set.of(1,2,3);
//		if(set1.contains(1))
//		JSONObject jsonObject1 = jsonObject.getJSONObject("");
//		jsonObject.getInt();
//		jsonObject.getLong();
//		jsonObject.getDouble();
//		jsonObject.getBoolean();
//		jsonObject.getString();


	}
}
