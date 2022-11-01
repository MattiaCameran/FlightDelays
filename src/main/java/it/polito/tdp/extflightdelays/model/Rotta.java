package it.polito.tdp.extflightdelays.model;

public class Rotta {
	private Airport a1;
	private Airport a2;
	private int nVoli;
	
	public Rotta(Airport a1, Airport a2, int nVoli) {
		super();
		this.a1 = a1;
		this.a2 = a2;
		this.nVoli = nVoli;
	}

	public Airport getA1() {
		return a1;
	}

	public void setA1(Airport a1) {
		this.a1 = a1;
	}

	public Airport getA2() {
		return a2;
	}

	public void setA2(Airport a2) {
		this.a2 = a2;
	}

	public int getnVoli() {
		return nVoli;
	}

	public void setnVoli(int nVoli) {
		this.nVoli = nVoli;
	}
	
	//Come per le adiacenze, non mi servono i metodi hashCode e equals() perché non dovrò mai confrontare
	//rotte tra loro, mi servono solo per recuperare i dati dal DB.
	
}
