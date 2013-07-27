package com.cloudwick.cassandra;
/*
 * Simple program to write a JSON file.
 */
import java.io.FileWriter;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonWriter {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		JSONObject obj = new JSONObject();

		obj.put("name", "varun");
		obj.put("age", new Integer(23));

		JSONArray list = new JSONArray();
		list.add("hyderabad");
		list.add("richmond");
		list.add("fremont");

		obj.put("places stayed", list);
		try {

			FileWriter file = new FileWriter("/Users/Chimmani/Desktop/test1.json");
			file.write(obj.toJSONString());
			file.flush();
			file.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.print(obj);

	}

}
