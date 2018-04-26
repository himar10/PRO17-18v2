package modelo;

import java.time.LocalDate;

public class Vehiculo {
	private int id;
	private String matricula;
	private String marcaModelo;
	private LocalDate fechaMatricula;
	private float precio;
	public Vehiculo(int id, String matricula, String marcaModelo, LocalDate fechaMatricula, float precio) {
		super();
		this.id = id;
		this.matricula = matricula;
		this.marcaModelo = marcaModelo;
		this.fechaMatricula = fechaMatricula;
		this.precio = precio;
	}
	public Vehiculo() {
		super();
		// TODO Auto-generated constructor stub
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getMatricula() {
		return matricula;
	}
	public void setMatricula(String matricula) {
		this.matricula = matricula;
	}
	public String getMarcaModelo() {
		return marcaModelo;
	}
	public void setMarcaModelo(String marcaModelo) {
		this.marcaModelo = marcaModelo;
	}
	public LocalDate getFechaMatricula() {
		return fechaMatricula;
	}
	public void setFechaMatricula(LocalDate fechaMatricula) {
		this.fechaMatricula = fechaMatricula;
	}
	public float getPrecio() {
		return precio;
	}
	public void setPrecio(float precio) {
		this.precio = precio;
	}
	
}
