package com.cloudwick.cassandra;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * This program parses a JSON file and inserts the data into Cassandra cluster
 * @author Varun
 */

public class YelpData {

	/**
	 * @param args
	 */

	private Cluster cluster;
	private Session session;

	public void connect(String node) {

		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}

		session = cluster.connect();

	}

	public void createSchema() {

		session.execute("CREATE KEYSPACE test2 WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};");

		session.execute("CREATE TABLE test2.business (business_id text,"
				+ "full_address text," + "open boolean,"
				+ "categories list<text>," + "city text,"
				+ "review_count double," + "name text PRIMARY KEY,"
				+ "neighborhoods list<text>," + "longitude double,"
				+ "state text," + "stars double," + "latitude double,"
				+ "type text" + ");");

	}

	public void loadData() {

		JSONParser parser = new JSONParser();

		try {

			BufferedReader br = new BufferedReader(new FileReader(
					"/Users/Chimmani/Desktop/yelp_business.json"));
			String line;
			int i = 1;
			while ((line = br.readLine()) != null) {
				// String line = br.readLine();

				Object obj = parser.parse(line);

				JSONObject jObject = (JSONObject) obj;

				String business_id = (String) jObject.get("business_id");
				String full_address = (String) jObject.get("full_address");
				Boolean open = (Boolean) jObject.get("open");
				JSONArray categories = (JSONArray) jObject.get("categories");
				String city = (String) jObject.get("city");
				Long review_count = (Long) jObject.get("review_count");
				String name = (String) jObject.get("name");
				JSONArray neighborhoods = (JSONArray) jObject
						.get("neighborhoods");
				Double longitude = (Double) jObject.get("longitude");
				String state = (String) jObject.get("state");
				Double stars = (Double) jObject.get("stars");
				Double latitude = (Double) jObject.get("latitude");
				String type = (String) jObject.get("type");

				System.out.println("VALUES ('"
						+ business_id
						+ "', '"
						+ full_address.replaceAll("'", "''")
						+ "', "
						+ open
						+ ", "
						+ categories.toString().replaceAll("'", "''")
								.replaceAll("\"", "'")
						+ ", '"
						+ city
						+ "', "
						+ review_count
						+ ", '"
						+ name.replaceAll("'", "''")
						+ "', "
						+ neighborhoods.toString().replaceAll("'", "''")
								.replaceAll("\"", "'") + ", " + longitude
						+ ", '" + state + "', " + stars + ", " + latitude
						+ ", '" + type + "'" + ");");

				session.execute("INSERT INTO test2.business (business_id, full_address, open, categories, city, review_count, name, neighborhoods, longitude, state, stars, latitude, type)"
						+ "VALUES ('"
						+ business_id
						+ "', '"
						+ full_address.replaceAll("'", "''")
						+ "', "
						+ open
						+ ", "
						+ categories.toString().replaceAll("'", "''")
								.replaceAll("\"", "'")
						+ ", '"
						+ city
						+ "', "
						+ review_count
						+ ", '"
						+ name.replaceAll("'", "''")
						+ "', "
						+ neighborhoods.toString().replaceAll("'", "''")
								.replaceAll("\"", "'")
						+ ", "
						+ longitude
						+ ", '"
						+ state
						+ "', "
						+ stars
						+ ", "
						+ latitude
						+ ", '" + type + "'" + ");");

				System.out.println("loaded row: " + i++);

			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void close() {
		cluster.shutdown();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		YelpData client = new YelpData();
		client.connect("192.168.1.121");
		client.createSchema();
		client.loadData();
		client.close();

	}

}
