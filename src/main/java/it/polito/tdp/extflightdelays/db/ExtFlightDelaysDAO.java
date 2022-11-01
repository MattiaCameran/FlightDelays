package it.polito.tdp.extflightdelays.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.extflightdelays.model.Airline;
import it.polito.tdp.extflightdelays.model.Airport;
import it.polito.tdp.extflightdelays.model.Flight;
import it.polito.tdp.extflightdelays.model.Rotta;

public class ExtFlightDelaysDAO {

	public List<Airline> loadAllAirlines() {
		String sql = "SELECT * from airlines";
		List<Airline> result = new ArrayList<Airline>();

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				result.add(new Airline(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRLINE")));
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	//Ho modificato il metodo del DAO per farmi caricare gli aeroporti nella mappa invece che farmi restituire una lista.
	public void loadAllAirports(Map<Integer,Airport> idMap) {
		String sql = "SELECT * FROM airports";

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				
				//Questa if serve a fare in modo che io carichi l'aeroporto nella mappa soltanto se già non è stato caricato. IMPORTANTE.
				if(!idMap.containsKey(rs.getInt("ID"))) {
					Airport airport = new Airport(rs.getInt("ID"), rs.getString("IATA_CODE"), rs.getString("AIRPORT"),
						rs.getString("CITY"), rs.getString("STATE"), rs.getString("COUNTRY"), rs.getDouble("LATITUDE"),
						rs.getDouble("LONGITUDE"), rs.getDouble("TIMEZONE_OFFSET"));
					//Inserisco nella mappa l'aeroporto con il suo ID e il suo oggetto stesso.
					idMap.put(airport.getId(), airport);
				}

			}

			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}

	public List<Flight> loadAllFlights() {
		String sql = "SELECT * FROM flights";
		List<Flight> result = new LinkedList<Flight>();

		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();

			while (rs.next()) {
				Flight flight = new Flight(rs.getInt("ID"), rs.getInt("AIRLINE_ID"), rs.getInt("FLIGHT_NUMBER"),
						rs.getString("TAIL_NUMBER"), rs.getInt("ORIGIN_AIRPORT_ID"),
						rs.getInt("DESTINATION_AIRPORT_ID"),
						rs.getTimestamp("SCHEDULED_DEPARTURE_DATE").toLocalDateTime(), rs.getDouble("DEPARTURE_DELAY"),
						rs.getDouble("ELAPSED_TIME"), rs.getInt("DISTANCE"),
						rs.getTimestamp("ARRIVAL_DATE").toLocalDateTime(), rs.getDouble("ARRIVAL_DELAY"));
				result.add(flight);
			}

			conn.close();
			return result;

		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	
	//Devo creare un nuovo metodo nel DAO che mi restituisca una lista di vertici filtrati in base al numero di compagnie aeree in cui sono coinvolti.
	//Devo passare al metodo come parametro anche la mappa perché io dalla query recupero gli ID e tramite la mappa dagli id passo agli oggetti che poi inserisco nella lista.
	public List<Airport> getVertici(int x, Map<Integer,Airport> idMap){
		
		String sql = "SELECT a.id "																//Selezionare gli id degli aeroporti.
				+ "FROM airports a, flights f "
				+ "WHERE (a.id = f.ORIGIN_AIRPORT_ID OR a.id = f.DESTINATION_AIRPORT_ID) "		//Dove l'id dell'aeroporto è uguale o all'id dell'aeroporto di partenza della tabella volo o a quello di arrivo della tabella volo.
				+ "GROUP BY a.id "																//Raggruppati in ordine di id. Ricorda: la tabella volo fornisce come parametri quali sono l'aeroporto di partenza e quello di arrivo del volo. Io so che un volo è coinvolo in una compagnia aerea se un volo parte OPPURE arriva a quell'aeroporto, quindi devo contarli entrambi.
				+ "HAVING COUNT(DISTINCT (f.AIRLINE_ID)) >= ?";									//Avendo una somma di DIFFERENTI compagnie aeree superiore a x fornito dall'utente.
		
		//Io quindi voglio sostanzialmente ottenere aeroporti da cui partano voli o arrivino voli e che abbiano al loro interno almeno x compagnie aeree diverse, che siano punti di partenza oppure punti di arrivo (se un aeroporto è punto di partenza per 5 compagnie e punto di arrivo per 6 e x = 10, io inserisco quell'aeroporto.
		
		List<Airport> result = new ArrayList<Airport> ();
		
		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setInt(1, x);
			ResultSet rs = st.executeQuery();
			
			while (rs.next()) {
				result.add(idMap.get(rs.getInt("id")));	//Noi inseriamo degli aeroporti presi dalla mappa di identità dove gli id sono uguali a quelli trovati come risultato dalla query del Database.
			}
			
			conn.close();
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
	}
	//Il metodo getRotte() ha bisogno come parametro della idMap perché devo poter recuperare l'oggetto aeroporto dall'id degli aeroporti
	//In questo caso io posso recuperarmi le rotte dalla tabella flights, selezionando l'aeroporto di partenza e arrivo e conto i voli (conto tutto, non più le compagnie).
	public List<Rotta> getRotte(Map<Integer, Airport> idMap) {
		String sql = "SELECT f.ORIGIN_AIRPORT_ID as a1, f.DESTINATION_AIRPORT_ID as a2, COUNT(*) AS n "
				+ "FROM flights f "
				+ "GROUP BY f.ORIGIN_AIRPORT_ID, f.DESTINATION_AIRPORT_ID";	//Raggruppo i voli per sorgente e destinazione
		List<Rotta> result = new ArrayList<Rotta>();
		
		try {
			Connection conn = ConnectDB.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				Airport sorgente = idMap.get(rs.getInt("a1"));			//Recupero i due oggetti Aeroporto
				Airport destinazione = idMap.get(rs.getInt("a2"));
				
				if(sorgente != null && destinazione != null) {			//Se i due oggetti non sono nulli, creo la rotta e l'aggiungo alla lista. Se il Database fosse stata manipolata e la rotta non fosse più presente. Un altro controllo sull'integrità del database.
					result.add(new Rotta(sorgente, destinazione, rs.getInt("n")));
				}
				
			}
			
			conn.close();
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Errore connessione al database");
			throw new RuntimeException("Error Connection Database");
		}
		
	}
	
	
	
	
	
	
	
	
	
}
