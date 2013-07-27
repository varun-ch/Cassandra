package com.cloudwick.cassandra;
/*
 * Simple program to read the contents of a JSON file
 */
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class JsonReader {

	/**
	 * @param args
	 * @throws org.json.simple.parser.ParseException
	 */
	public static void main(String[] args) throws ParseException,
			org.json.simple.parser.ParseException {
		// TODO Auto-generated method stub

		JSONParser parser = new JSONParser();

		try {

			Object obj = parser.parse(new FileReader(
					"/Users/Chimmani/Desktop/test1.json"));

			JSONObject jsonObject = (JSONObject) obj;

			String name = (String) jsonObject.get("name");
			System.out.println(name);

			long age = (Long) jsonObject.get("age");
			System.out.println(age);

			// loop array
			JSONArray places = (JSONArray) jsonObject.get("places stayed");
			Iterator<String> iterator = places.iterator();
			while (iterator.hasNext()) {
				System.out.println(iterator.next());
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}