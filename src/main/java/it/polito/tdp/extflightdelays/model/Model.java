package it.polito.tdp.extflightdelays.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import it.polito.tdp.extflightdelays.db.ExtFlightDelaysDAO;

public class Model {
	
	//La prima cosa di cui sicuramente abbiamo bisogno è il grafo, che creo.
	private Graph<Airport, DefaultWeightedEdge> grafo;
	
	private ExtFlightDelaysDAO dao;	//La seconda cosa da fare è collegare il modello al DAO.
	
	private Map<Integer, Airport> idMap;	//Può essere utile inserire comunque la idMap per poter ottenere gli aerei facilmente dai loro identificativi.
	
	//Ora devo decidere cosa mi serve inizializzare subito nel costruttore del modello.
	
	public Model() {
		
		//Un caso è sicuramente quello del DAO
		dao = new ExtFlightDelaysDAO();
		
		//Un altro caso è la idMap, in quanto gli id per ogni aereo rimarranno sempre quelli. Io me li metto da parte all'inizio, facendo sforzi subito e la utilizzo poi quando mi serve.
		idMap = new HashMap<Integer, Airport>();	//Avere una mappa che tiene conto di tutti gli aeroporti semplifica le interazioni in caso in cui l'utente voglia cliccare più volte su crea grafo.
		
		dao.loadAllAirports(idMap); 	//Come appena detto, carico subito nel costruttore tutti gli aeroporti del database nella mappa con i loro Id.
	}
	
	//Ho creato il grafo, il riferimento al dao e la mappa, li ho inizializzati nel costruttore e ho riempito la mappa, è tempo di implementare il metodo di creazione del Grafo.
	
	public void creaGrafo(int x) {	//NOTA IMPORTANTE: a differenza degli esercizi precedenti, questo mi chiede di caricare nel grafo vertici che devono avere un numero di compagnie minime. x è quindi il filtro sui vertici.
		
		grafo = new SimpleWeightedGraph<Airport, DefaultWeightedEdge>(DefaultWeightedEdge.class);	//Mi creo il grafo secondo le necessità mie: pesato, semplice e non ordinato.
		
		//Ora devo aggiungere i vertici. 
		
		//Ho tutti gli aereoporti, ma mi serve solo un sottoinsieme di questi. Mi interessano gli aeroporti su cui operano almeno x compagnie aeree.
		//Ho fatto la query e creato il metodo nel DAO, recupero ora i vertici.
		Graphs.addAllVertices(this.grafo, dao.getVertici(x, idMap));
		
		//Ora aggiungo gli archi.
		//NOTA IMPORTANTE: in questo esercizio a me interessano sia le informazioni dal vertice a al vertice b sia il contrario (es: i voli Torino-Catania sono diversi dai voli Catania-Torino, devo considerarli entrambi).
		
		//Il modo più semplice è di recuperare dal Database tutte le rotte da a verso b e da b verso a, le recupero dal Database e le elaboro con il codice.
		//Devo creare nel package Model una nuova classe che chiamo rotta. In questa dovrò mettere due attributi Airport (partenza e arrivo) e un peso (che sarà il peso di un verso).
		
		
		//Utilizzo un metodo TOP-DOWN: suppongo di avere già un metodo che mi ritorni le rotte. Come le gestisco?
		
		//Io vado a vedere le rotte aventi l'aeroporto a1, l'aeroporto a2 e vedo se c'è già l'arco. Se non c'è lo aggiungo.
		//Se poi trovo la rotta inversa aumento il peso.
		
		for(Rotta r: dao.getRotte(idMap)) {	//Prendo le rotte
			
			if(this.grafo.containsVertex(r.getA1()) && this.grafo.containsVertex(r.getA2())) {	//Questa if mi assicura che i vertici siano presenti nel database (i vertici li ho aggiunti dal DB). E' semplicemente un controllo sul fatto che il DB sia fatto bene.
			DefaultWeightedEdge edge = this.grafo.getEdge(r.getA1(), r.getA2());
			
			if(edge == null) {	//Non c'è l'arco, lo creo e lo aggiungo al grafo.
				
				Graphs.addEdgeWithVertices(this.grafo, r.getA1(), r.getA2(), r.getnVoli());
			}
			else {	//L'arco c'era già. Mi recupero quindi il peso che c'era già e lo aggiorno.
				
				double pesoVecchio = this.grafo.getEdgeWeight(edge);	//Prendo il "peso vecchio" ovvero il peso che avevo prima di visualizzare la rotta interna (es: quanti voli Torino Catania erano stati fatti).
				double pesoNuovo = pesoVecchio + r.getnVoli();			//Aggiorno con il peso nuovo dato dal peso vecchio (numero voli Torino Catania) più il numero di voli della rotta corrente di cui l'arco è lo stesso (numero voli Catania - Torino).
				this.grafo.setEdgeWeight(edge, pesoNuovo);				//Aggiorno l'arco nel grafo.
				}
			}
		}
		
		//Ho creato gli archi. Posso fare dei metodi di ritorno della size dei vertici e degli archi per facilitare il recupero da parte del controller e della stampa.
	}
	
	//Devo creare un metodo che restituisca tutti gli aeroporti in modo ordinato perché questi andranno a popolare la tendina dell'interfaccia.
	public List<Airport> getVertici(){
		
		List<Airport> lTemp = new ArrayList<Airport>(this.grafo.vertexSet());	//Devo ordinare la lista che restituisco
		Collections.sort(lTemp);
		return lTemp;
	}
	
	
	public List<Airport> getPercorso(Airport a1, Airport a2){
		
		
		//Partiamo da a1, visitiamo il grafo e poi utilizziamo il metodo getParent() per arrivare dalla destinazione alla sorgente.
		
		//Creo la lista del percorso inizialmente vuota.
		List<Airport> percorso = new ArrayList<Airport>();
		BreadthFirstIterator<Airport, DefaultWeightedEdge> it = new BreadthFirstIterator<>(this.grafo, a1);		//Creo l'iteratore che mi permetta di visitare in ampiezza il mio grafo passandogli a1 come vertice radice (devo indicare il tipo di vertici e archi).
		
		Boolean trovato = false; 	//Booleano per capire se trovo a2.
		
		//Ora che ho l'iteratore, posso visitare l'arco.
		while(it.hasNext()) {	//Finché l'iteratore ha un arco successivo a sè.
			Airport visitato = it.next();			//Visitiamo il nodo. it.next() mi ritorna l'aeroporto su cui si trova, che sta visitando.
			if(visitato.equals(a2)) {				//Se mi trovo sull'aeroporto a2, ho trovato, booleano a vero.
				trovato = true;
			}
		}
		
		//Ho visitato il grafo, ora devo prima di tutto controllare che a2 sia presente nel grafo, quindi che i due aeroporti siano effettivamente collegati.
		
		if(trovato) {
		//Ora posso ottenere il percorso.
		//L'idea è di partire dalla destinazione alla sorgente, quindi mi conviene aggiungere una lista dove aggiungo sempre in testa.
		percorso.add(a2);		//Aggiungo la destinazione.
		
		Airport step = it.getParent(a2);	//Passo al nodo parente della destinazione (faccio uno step verso l'alto).
		
		while(!step.equals(a1)) {			//Fino a quando io non sono risalito alla radice, aggiungo in testa l'aeroporto al percorso.
			percorso.add(0, step);
			step = it.getParent(step);		//Dopo che ho aggiunto, passo al vertice padre.
		}
		
		percorso.add(0, a1);				//A causa del mio while, io non ho aggiunto al percorso il vertice radice.
		}
		
		return percorso;
		
	}

	public int nVertici() {
		
		return this.grafo.vertexSet().size();
	}

	public int nArchi() {
		
		return this.grafo.edgeSet().size();
	}
}