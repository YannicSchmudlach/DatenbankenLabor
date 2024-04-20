
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import de.hska.iwii.db1.weather.model.Weather;
import de.hska.iwii.db1.weather.model.WeatherForecast;
import de.hska.iwii.db1.weather.reader.WeatherReader;

/**
 * Demo-Klasse fuer den Zugriff auf das Wetter der Stadt Karlsruhe. 
 */
public class DemoWeather {
	private ArrayList<Integer>stationIDs=new ArrayList<>();

	public ArrayList<Integer> getStationIDs() {
		return stationIDs;
	}

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
		// 1. Erzeugt ein WeatherReader-Objekt fuer die komplette
		//    Serverkommunikation. Fuer den Zugriff uber den
		//    Proxy der Hochschule muss der zweite Konstruktur mit
		//    den Proxy-Parametern verwendet werden.
		//    Proxy-Server: proxy.hs-karlsruhe.de
		//    Port des Proxy-Servers: 8888
		WeatherReader reader = new WeatherReader();
		
		// 2. Auslesen von Informationen ueber einen oder mehrere Orte.
		// Die Liste der Stationen ist hier verlinkt (4. Spalte enthaelt die ID):
		// https://www.dwd.de/DE/leistungen/klimadatendeutschland/statliste/statlex_html.html
		// Beispiele:
		// 10519: Karlsruhe Durlach
		// 10321: Stuttgart-Degerloch
		// Direktes Aufrufen der API:
		// https://dwd.api.bund.dev/

		DemoWeather demoWeather = new DemoWeather();
		demoWeather.stationIDs.add(10519);
		demoWeather.stationIDs.add(10321);
		demoWeather.stationIDs.add(10218);
		demoWeather.stationIDs.add(10517);
		demoWeather.stationIDs.add(10516);

		Connection conn=getMySQLConnection("root","","BonusAufgabe");

		insertWeatherIntoDatabase(conn,demoWeather.getStationIDs(),reader);
		getAllFromStationId(10519,conn);
		getStationIdsBetween(conn,new Date(2022-12-12),-60,12);
		/*
		WeatherForecast forecast = reader.readWeatherForecast(10519);
		if (forecast != null) {
			for (Weather weather: forecast.getWeather()) {
				System.out.println(weather.getDate() + ", " + weather.getMinTemp() / 10.0 + ", " + weather.getMaxTemp() / 10.0);
			}
		}*/
		conn.close();
	}
	public static Connection getMySQLConnection(String databaseUser, String databasePassword, String databaseName) throws ClassNotFoundException, SQLException {
		// MySDQL
		Class.forName("com.mysql.cj.jdbc.Driver");

		// 2. Verbinden mit Anmelde-Daten
		Properties props = new Properties();
		props.put("user", databaseUser);
		props.put("password", databasePassword);
		return DriverManager.getConnection("jdbc:mysql://localhost:3306/" + databaseName + "?serverTimezone=Europe/Berlin", props);
	}
	public static void insertWeatherIntoDatabase(Connection con, ArrayList<Integer> stationID,WeatherReader reader) throws SQLException {

		String sql="INSERT INTO weather(stationId,dayDate,temperatureMin,temperatureMax,precipitation,sunshine)VALUES(?,?,?,?,?,?)";
		PreparedStatement preparedStatement=con.prepareStatement(sql);
		preparedStatement.clearBatch();
		for (int id: stationID){
			WeatherForecast forecast = reader.readWeatherForecast(id);
			if (forecast!=null){
				for (Weather weather: forecast.getWeather()) {
					preparedStatement.setInt(1,id);
					preparedStatement.setDate(2, weather.getDate());
					preparedStatement.setFloat(3,weather.getMinTemp());
					preparedStatement.setFloat(4,weather.getMaxTemp());
					preparedStatement.setInt(5,weather.getPrecipitation());
					preparedStatement.setInt(6,weather.getSunshine());
					preparedStatement.addBatch();
				}

			}
		}
		int a[] =preparedStatement.executeBatch();
		con.commit();
	}
	public static void getAllFromStationId(int stationId,Connection conn) throws SQLException {
		PreparedStatement preparedStatement=conn.prepareStatement("SELECT * FROM weather WHERE stationId=?");
		preparedStatement.setInt(1,stationId);
		ResultSet rs=preparedStatement.executeQuery();

		while(rs.next()){
			System.out.println(rs.getInt("id") +"\t"+rs.getInt("stationId")+"\t"+rs.getString("dayDate")+"\t"+rs.getFloat("temperatureMin")+"\t"+rs.getString("temperatureMax")+"\t"+rs.getInt("precipitation")+"\t"+rs.getInt("sunshine"));
		}
	}
	public static void getStationIdsBetween(Connection conn, Date date, int low, int high) throws SQLException {
		PreparedStatement preparedStatement=conn.prepareStatement("SELECT stationID FROM weather WHERE dayDate=? and temperatureMax BETWEEN ? AND ?");
		preparedStatement.setDate(1,date);
		preparedStatement.setFloat(2,low);
		preparedStatement.setFloat(3,high);
		ResultSet rs = preparedStatement.executeQuery();

		while(rs.next()){
			System.out.println(rs.getInt("id") +"\t"+rs.getInt("stationId"));
		}
	}
}
