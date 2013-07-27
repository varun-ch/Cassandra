package com.cloudwick.cassandra;
/*
 * Program to connect to Cassandra cluster, creating a keyspace, creating columnfamilies and inserting data.
 */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SimpleClient {
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

		session.execute("CREATE KEYSPACE simplex WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':3};");

		session.execute("CREATE TABLE simplex.songs (" + "id uuid PRIMARY KEY,"
				+ "title text," + "album text," + "artist text,"
				+ "tags set<text>," + "data blob" + ");");

		session.execute("CREATE TABLE simplex.playlists (" + "id uuid,"
				+ "title text," + "album text, " + "artist text,"
				+ "song_id uuid," + "PRIMARY KEY (id, title, album, artist)"
				+ ");");
	}

	public void loadData() {

		session.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) "
				+ "VALUES ("
				+ "756716f7-2e54-4715-9f00-91dcbea6cf50,"
				+ "'La Petite Tonkinoise',"
				+ "'Bye Bye Blackbird',"
				+ "'Jos�phine Baker'," + "{'jazz', '2013'})" + ";");
		session.execute("INSERT INTO simplex.playlists (id, song_id, title, album, artist) "
				+ "VALUES ("
				+ "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,"
				+ "756716f7-2e54-4715-9f00-91dcbea6cf50,"
				+ "'La Petite Tonkinoise',"
				+ "'Bye Bye Blackbird',"
				+ "'Jos�phine Baker'" + ");");
		
		String title = "the varun fever";
		String album = "inner voice";
		String artist ="varun";
		
		session.execute("INSERT INTO simplex.songs (id, title, album, artist, tags) "+ "VALUES ("
				+ "756716f7-2e54-4715-9f00-91dcbe65cf50,"
				+ "'"+title+"',"
				+ "'"+album+"',"
				+ "'"+artist+"'," + "{'jazz', '2013'})" + ";");
		

	}

	public void querySchema() {

		ResultSet results = session.execute("SELECT * FROM simplex.playlists "
				+ "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");

		System.out
				.println(String
						.format("%-30s\t%-20s\t%-20s\n%s", "title", "album",
								"artist",
								"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s",
					row.getString("title"), row.getString("album"),
					row.getString("artist")));
		}
		System.out.println();
	}

	public void close() {
		cluster.shutdown();
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		client.connect("192.168.1.121");
		 client.createSchema();
		 client.loadData();
		client.querySchema();
		client.close();
	}

}