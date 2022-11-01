package it.polito.tdp.extflightdelays;

import java.net.URL;
import java.util.List;
import java.util.ResourceBundle;

import it.polito.tdp.extflightdelays.model.Airport;
import it.polito.tdp.extflightdelays.model.Model;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;

public class FXMLController {
	
	private Model model;

    @FXML // ResourceBundle that was given to the FXMLLoader
    private ResourceBundle resources;

    @FXML // URL location of the FXML file that was given to the FXMLLoader
    private URL location;

    @FXML // fx:id="txtResult"
    private TextArea txtResult; // Value injected by FXMLLoader

    @FXML // fx:id="compagnieMinimo"
    private TextField compagnieMinimo; // Value injected by FXMLLoader

    @FXML // fx:id="cmbBoxAeroportoPartenza"
    private ComboBox<Airport> cmbBoxAeroportoPartenza; // Value injected by FXMLLoader

    @FXML // fx:id="cmbBoxAeroportoDestinazione"
    private ComboBox<Airport> cmbBoxAeroportoDestinazione; // Value injected by FXMLLoader

    @FXML // fx:id="btnAnalizza"
    private Button btnAnalizza; // Value injected by FXMLLoader

    @FXML // fx:id="btnConnessione"
    private Button btnConnessione; // Value injected by FXMLLoader

    @FXML
    void doAnalizzaAeroporti(ActionEvent event) {
    	txtResult.clear();
    	
    	//Recupero l'input con il solito pattern.
    	int x;
    	
    	try {
    		x = Integer.parseInt(compagnieMinimo.getText());	//Prendo il risultato dal textfield e provo a convertirlo in intero.
    	} catch (NumberFormatException e) {
    		txtResult.appendText("Inserire valore numerico");
    		return;
    	}
    	
    	this.model.creaGrafo(x);
    	
    	txtResult.appendText("# VERTICI: " + this.model.nVertici() + "\n");
    	txtResult.appendText("# ARCHI: " + this.model.nArchi());
    	
    	cmbBoxAeroportoPartenza.getItems().addAll(this.model.getVertici());		//Riempio le due tendine con tutti gli aeroporti (vertici del grafo). Ricordarsi il getItems() per passare da oggetti di codice a oggetti di interfaccia.
    	cmbBoxAeroportoDestinazione.getItems().addAll(this.model.getVertici());	//Ho riempito le tendine con i codici dell'aeroporto ordinati alfabeticamente.
    }

    @FXML
    void doTestConnessione(ActionEvent event) {
    	txtResult.clear();
    	if(cmbBoxAeroportoPartenza.getValue() == null ||
    			cmbBoxAeroportoDestinazione.getValue() == null) {
    		txtResult.appendText("Seleziona i due aeroporti!");
    		return ;
    	}
    	
    	//Provo a stampare a console un percorso.
    	List<Airport> percorso = this.model.getPercorso(cmbBoxAeroportoPartenza.getValue(), cmbBoxAeroportoDestinazione.getValue());
    	
    	if(percorso == null) {
    		txtResult.appendText("I due aeroporti non sono collegati");
    	} else {
    		txtResult.appendText(percorso.toString());
    	}
    }

    @FXML // This method is called by the FXMLLoader when initialization is complete
    void initialize() {
        assert txtResult != null : "fx:id=\"txtResult\" was not injected: check your FXML file 'Scene.fxml'.";
        assert compagnieMinimo != null : "fx:id=\"compagnieMinimo\" was not injected: check your FXML file 'Scene.fxml'.";
        assert cmbBoxAeroportoPartenza != null : "fx:id=\"cmbBoxAeroportoPartenza\" was not injected: check your FXML file 'Scene.fxml'.";
        assert cmbBoxAeroportoDestinazione != null : "fx:id=\"cmbBoxAeroportoDestinazione\" was not injected: check your FXML file 'Scene.fxml'.";
        assert btnAnalizza != null : "fx:id=\"btnAnalizza\" was not injected: check your FXML file 'Scene.fxml'.";
        assert btnConnessione != null : "fx:id=\"btnConnessione\" was not injected: check your FXML file 'Scene.fxml'.";

    }
    
    public void setModel(Model model) {
    	this.model = model;
    }
}
