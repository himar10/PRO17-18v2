package modelo;

import java.io.Serializable;
import java.time.LocalDate;

public class Estudiante extends Persona implements Comparable<Estudiante> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int codGrupo;

	public Estudiante(int codGrupo) {
		super();
		this.codGrupo = codGrupo;
	}

	public Estudiante(int codGrupo, String nif, String nombre, char sexo, LocalDate fecha, int altura, Persona padre, Persona madre) {
		
		super(nif,nombre, sexo, fecha, altura, padre, madre);
		this.codGrupo = codGrupo;
		//System.out.println("construyendo estudiante");
	}
	public String toString() {
		
		return super.toString() + " , codGrupo : " + this.codGrupo;
	}

	public int getCodGrupo() {
		return codGrupo;
	}

	public void setCodGrupo(int codGrupo) {
		this.codGrupo = codGrupo;
	}

	
	public int compareTo(Estudiante estudiante) {
		return this.getNif().compareTo(estudiante.getNif());
		
	}

	

}
