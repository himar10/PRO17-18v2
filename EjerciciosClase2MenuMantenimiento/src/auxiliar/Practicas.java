package auxiliar;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.TypeVariable;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;

import modelo.Datos;
import modelo.Equipo;
import modelo.Estudiante;

public class Practicas {

	// TERCERA EVALUACION
	// Leer datos desde teclado...
	public void menu() {
		String tecleado = "XYZ";
		while (tecleado.compareToIgnoreCase("q") != 0) {

			System.out.println("\t\t MENU PRINCIPAL");
			System.out.println("\t\t 1.Opcion 1");
			System.out.println("\t\t 2.Opcion 2");
			System.out.println("\t\t 3.Opcion 3");
			System.out.println("\t\t 4.Opcion 4");
			System.out.println("\t\t q,Q SALIR");
			Scanner teclado = new Scanner(System.in);
			tecleado = teclado.nextLine();
			System.out.println("Usted ha tecleado .. " + tecleado);
		}
		
		System.out.println("Hasta la proxima");

	}
	// SEGUNDA EVALUACION
	// LISTADO DE PROVINCIAS POR CCAA.

	public String[] leerCA(String rutaComunidades) {
		String[] comunidades = new String[19];
		try {
			// Abrir el fichero
			FileReader fr = new FileReader(rutaComunidades);
			BufferedReader br = new BufferedReader(fr);
			String linea;
			int i = 0;
			while ((linea = br.readLine()) != null)
				comunidades[i++] = linea.split("%")[1];
			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return comunidades;
	}

	public HashMap<String, ArrayList<String>> generarDatosListadoProvincias(String rutaProvincias) {
		String[] comunidades = leerCA("ficheros/comunidades.txt");
		HashMap<String, ArrayList<String>> datosListado = new HashMap<String, ArrayList<String>>();
		try {
			// Abrir el fichero
			FileReader fr = new FileReader(rutaProvincias);
			BufferedReader br = new BufferedReader(fr);
			String linea;
			int i = 0;
			while ((linea = br.readLine()) != null) {
				String[] campos = linea.split("%");
				if (datosListado.get(comunidades[Integer.parseInt(campos[2]) - 1]) == null)
					datosListado.put(comunidades[Integer.parseInt(campos[2]) - 1], new ArrayList<String>());
				datosListado.get(comunidades[Integer.parseInt(campos[2]) - 1]).add(campos[1] + "#" + campos[3]);

			}
			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return datosListado;
	}

	public void listadoProvinciasPorCA(HashMap<String, ArrayList<String>> datosListado) {
		// recorrer hm de entrada creando el de salida
		Set<String> comunidades = datosListado.keySet();
		for (String comunidad : comunidades) {
			ArrayList<String> listaProvincias = datosListado.get(comunidad);
			int acumuladoCA = 0;
			System.out.println("CCAA : " + comunidad);
			for (String provincia : listaProvincias) {
				acumuladoCA += Integer.parseInt(provincia.split("#")[1]);
				System.out.println(provincia.split("#")[0] + ", " + provincia.split("#")[1]);
			}
			System.out.println("Total padrón CCAA : " + comunidad + " = " + acumuladoCA);
		}
	}

	public ArrayList<Estudiante> introListas() {
		ArrayList<Estudiante> listaE;
		listaE = new ArrayList<Estudiante>();
		Estudiante est1 = new Estudiante(123);
		listaE.add(est1);
		listaE.add(est1);
		listaE.add(est1);
		listaE.add(est1);
		listaE.add(est1);
		int tam = listaE.size();
		Estudiante est2 = new Estudiante(321);
		listaE.add(0, est2);
		listaE.remove(listaE.size() - 1);
		// listaE.set(0, est1);
		for (Estudiante estudiante : listaE) {
			// System.out.println(estudiante.getCodGrupo());
		}
		for (int i = 0; i < listaE.size(); i++) {
			// System.out.println(listaE.get(i).getCodGrupo());
		}

		// System.out.println("fin introListas");
		return listaE;

	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
		list.sort((o1, o2) -> o1.getValue().compareTo(o2.getValue()));

		Map<K, V> result = new LinkedHashMap<>();
		for (Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
	}

	// 11 enero 2018
	// Leer una matriz de int y devolverla como ArrayList

	public ArrayList<ArrayList<Integer>> convierteMatrizArrayLista(int[][] matriz) {

		ArrayList<ArrayList<Integer>> resultado = new ArrayList<ArrayList<Integer>>();
		for (int[] filaMatriz : matriz) {
			// crear alist
			ArrayList<Integer> filaLista = new ArrayList<Integer>();
			for (int numero : filaMatriz)
				filaLista.add(numero);
			resultado.add(filaLista);
		}
		return resultado;
	}

	// Mapas, clase HashMap

	public HashMap<String, Estudiante> introMapas() {
		// la clave representa el nif del Estudiante
		HashMap<String, Estudiante> resultado = new HashMap<String, Estudiante>();
		Estudiante est = new Estudiante(123, "435G", "Paco", 'M', null, 180, null, null);
		resultado.put(est.getNif(), est);
		Estudiante estudiante = resultado.get("435G");
		Estudiante est2 = new Estudiante(321, "435G", "Carlos", 'M', null, 180, null, null);

		resultado.put("435G", est2);
		resultado.put("123T", new Estudiante(123, "123T", "Pepe", 'M', null, 180, null, null));
		Set<String> claves = resultado.keySet();
		for (String clave : claves) {
			// System.out.println(resultado.get(clave).getNombre());
		}

		return resultado;
	}

	public Estudiante crearEstudianteLeido(String[] datos) {
		int grupo = Integer.parseInt(datos[0]);
		Estudiante estudiante = new Estudiante(grupo);
		estudiante.setNif(datos[1]);
		estudiante.setNombre(datos[2]);
		estudiante.setSexo(datos[3].charAt(0));
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");
		LocalDate fechaNac = LocalDate.parse(datos[4], fmt);
		estudiante.setFecha(fechaNac);
		estudiante.setAltura(Integer.parseInt(datos[5]));
		estudiante.setMadre(null);
		estudiante.setPadre(null);
		return estudiante;

	}

	public void copiaEstudiantesTxtAObjetos(String rutaTxt, String rutaObj) {
		try {
			// Abrir el fichero entrada
			FileReader fr = new FileReader(rutaTxt);
			BufferedReader br = new BufferedReader(fr);
			// Abrir el fichero salida
			FileOutputStream fIs = new FileOutputStream(rutaObj);
			ObjectOutputStream fObj = new ObjectOutputStream(fIs);

			String linea;
			// System.out.println(LocalDate.now());
			// Leer el fichero linea a linea
			while ((linea = br.readLine()) != null) {
				String[] campos = linea.split("#");
				// crear estudiante a partir del registro leido
				Estudiante estudiante = crearEstudianteLeido(campos);
				// grabar objeto estudiante en fichero ..
				fObj.writeObject(estudiante);

			}
			fr.close();
			br.close();
			fIs.close();
			fObj.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

	}

	public void copiaEstudiantesObjATxt(String rutaObj, String rutaTxt) {
		try {
			FileInputStream fIs = new FileInputStream(rutaObj);
			ObjectInputStream fObj = new ObjectInputStream(fIs);
			BufferedWriter fb = new BufferedWriter(new FileWriter(rutaTxt));

			// recorrer el fichero
			Estudiante est = null;

			while (fIs.available() > 0) {
				String registroTxt = "";
				est = (Estudiante) fObj.readObject();
				registroTxt = est.getCodGrupo() + "#" + est.getNif() + "#" + est.getNombre() + "#" + est.getSexo() + "#"
						+ est.getFecha().toString() + "#" + est.getAltura() + "\n";
				fb.write(registroTxt);
			}
			fb.close();
			fIs.close();
			fObj.close();
		} catch (FileNotFoundException e) {
			System.out.println("Fichero no encontrado");
		} catch (IOException e) {
			System.out.println("Error IO");
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFound");
		}
	}

	public static void grabarObjetosEnFichero(String fichero) {
		Estudiante est = new Estudiante(10, "111G", "Paco1", 'M', null, 181, null, null);
		Estudiante est1 = new Estudiante(20, "222G", "Paco2", 'M', null, 180, null, null);
		Estudiante est2 = new Estudiante(30, "333G", "Paco3", 'M', null, 180, null, null);
		ArrayList<Estudiante> lista = new ArrayList<Estudiante>();

		// añadimos los 3 estudiantes a la lista

		lista.add(est);
		lista.add(est1);
		lista.add(est2);

		// abrir el fichero de objetos...
		try {
			FileOutputStream fIs = new FileOutputStream(fichero);
			ObjectOutputStream fObj = new ObjectOutputStream(fIs);

			// guardar los objetos estudiantes en el fichero...
			// fObj.writeObject(lista);
			fObj.writeObject(est);
			fObj.writeObject(est1);
			fObj.writeObject(est2);
			fObj.close();
			fIs.close();
		} catch (FileNotFoundException e) {
			System.out.println("Fichero no encontrado");
		} catch (IOException e) {
			System.out.println("Error IO");

		}
		System.out.println("Fichero creado");
	}

	public ArrayList<Estudiante> leeObjetosDesdeFichero(String fichero) {
		ArrayList<Estudiante> lista = null;
		try {
			FileInputStream fIs = new FileInputStream(fichero);
			ObjectInputStream fObj = new ObjectInputStream(fIs);

			// recorrer el fichero
			Estudiante est = null;

			while (fIs.available() > 0) {
				// est = (Estudiante) fObj.readObject();
				lista = (ArrayList<Estudiante>) fObj.readObject();

				// System.out.println(est.getCodGrupo() + ", " + est.getNombre());
			}
			// System.out.println(lista.get(0).getNombre());
			fIs.close();
			fObj.close();
		} catch (FileNotFoundException e) {
			System.out.println("Fichero no encontrado");
		} catch (IOException e) {
			System.out.println("Error IO");
		} catch (ClassNotFoundException e) {
			System.out.println("ClassNotFound");
		}
		return lista;
	}

	public void leerFicheroTexto() {
		try {
			// Abrir el fichero
			FileReader fr = new FileReader("ficheros/personas.txt");
			BufferedReader br = new BufferedReader(fr);
			String linea;
			// System.out.println(LocalDate.now());
			// Leer el fichero linea a linea
			while ((linea = br.readLine()) != null) {

				String[] campos = linea.split("&&");
				System.out.println(linea);
				System.out.println(calculaEdad(campos[2]));

			}
			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void leerFicheroTextoOrdenadoClave(String rutaFichero) {
		try {
			// Abrir el fichero
			FileReader fr = new FileReader(rutaFichero);
			BufferedReader br = new BufferedReader(fr);
			String codigo_leido, codigo_anterior = null;
			int contador_grupo = 0;
			int contador_total = 0;
			String linea;
			// Leer el fichero linea a linea
			while ((linea = br.readLine()) != null) {
				String[] campos = linea.split("&&");
				codigo_leido = campos[0];
				if (codigo_anterior == null) // primer registro
					codigo_anterior = codigo_leido;
				if (!codigo_leido.equals(codigo_anterior)) {
					System.out.println("Hay " + contador_grupo + " alumnos en el grupo " + codigo_anterior);
					contador_total += contador_grupo;
					contador_grupo = 0;
					codigo_anterior = codigo_leido;
				}
				contador_grupo++;
			}
			System.out.println("Hay " + contador_grupo + " alumnos en el grupo " + codigo_anterior);
			contador_total += contador_grupo;
			System.out.println("Hay " + contador_total + " alumnos en total ");

			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public HashMap<String, ArrayList<Float>> resumenVentasVendedor(String ficheroVentas) {
		HashMap<String, ArrayList<Float>> resultado = new HashMap<String, ArrayList<Float>>();

		try {

			// Abrir el fichero
			FileReader fr = new FileReader(ficheroVentas);
			BufferedReader br = new BufferedReader(fr);
			String linea;
			int acumulado = 0;
			int contador = 0;
			while ((linea = br.readLine()) != null) {
				String[] campos = linea.split("%");
				if (resultado.get(campos[1]) == null)
					resultado.put(campos[1], new ArrayList<Float>());
				resultado.get(campos[1]).add(Float.parseFloat((campos[2])));
			}

			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return resultado;
	}

	public void inicializaVisitantesIsla(HashMap<Integer, ArrayList<Float>> resultado) {
		ArrayList<Float> visitantesMeses;
		for (int isla = 0; isla < 7; isla++) { // recorre cada isla
			visitantesMeses = new ArrayList<Float>();
			for (int mes = 0; mes < 12; mes++) // poner a 0 cada uno de los meses
				visitantesMeses.add(0f);
			resultado.put(isla, visitantesMeses);
		}
	}

	public HashMap<Integer, ArrayList<Float>> visitantesIslaMes(String rutaFicheroVisitantes) {
		HashMap<Integer, ArrayList<Float>> resultado = new HashMap<Integer, ArrayList<Float>>();
		try {
			// Abrir el fichero
			FileReader fr = new FileReader(rutaFicheroVisitantes);
			BufferedReader br = new BufferedReader(fr);
			String linea;
			inicializaVisitantesIsla(resultado);
			// Leer el fichero linea a linea
			while ((linea = br.readLine()) != null) {

				String[] campos = linea.split("@");
				int isla = Integer.parseInt(campos[0]);
				int mes = Integer.parseInt(campos[1]);
				float numeroVisitantes = Float.parseFloat(campos[2]);
				resultado.get(isla - 1).set(mes - 1, numeroVisitantes);

			}
			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
		return resultado;
	}

	public void listadoIslasMeses(String rutaFicheroVisitantes) {
		ArrayList<Float> visitantesIsla;
		HashMap<Integer, ArrayList<Float>> hm = visitantesIslaMes(rutaFicheroVisitantes);

		String[] islas = { "GRAN CANARIA", "LANZAROTE", "FUERTEVENTURA", "TENERIFE", "LA PALMA", "LA GOMERA",
				"EL HIERRO" };
		String[] meses = { "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIE",
				"OCTUBRE", "NOVIEM", "DICIEMBRE" };

		// recorrer hm
		float acumuladoMes[] = new float[12];
		Set<Integer> claves = hm.keySet();
		System.out.print("\t\t");
		for (int i = 0; i < meses.length; i++) {
			System.out.print(meses[i] + "\t");
		}
		System.out.println();
		for (Integer clave : claves) {
			// islas[clave]
			visitantesIsla = hm.get(clave);
			System.out.print(islas[clave] + "\t");
			float acumuladoIsla = 0f;
			for (int i = 0; i < visitantesIsla.size(); i++) {
				acumuladoIsla += visitantesIsla.get(i);
				acumuladoMes[i] += visitantesIsla.get(i);
				System.out.printf("%.0f\t", visitantesIsla.get(i) * 1000);
			}
			System.out.print("\t total visitantes " + islas[clave] + " = " + acumuladoIsla);
			System.out.println();
		}
		for (Float valor : acumuladoMes) {
			System.out.print("\t\t" + valor);
		}
	}

	public void generaFicheroLanzamientosDado(int cuantos, String rutaFichero) {
		try {
			BufferedWriter fb = new BufferedWriter(new FileWriter(rutaFichero));
			Random rnd = new Random();
			for (int i = 0; i < cuantos; i++) {
				int numero = 1 + rnd.nextInt(6);
				String registro = System.currentTimeMillis() + "#" + i + "#" + numero + "\n";
				int retardo = 1 + rnd.nextInt(1000);
				Thread.sleep(retardo);
				fb.write(registro);
			}
			fb.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public HashMap<String, Float> resumenVentasPorVendedor(HashMap<String, ArrayList<Float>> ventas) {
		HashMap<String, Float> resultado = new HashMap<String, Float>();
		// recorrer hm de entrada creando el de salida
		Set<String> claves = ventas.keySet();
		for (String clave : claves) {
			ArrayList<Float> listaVentas = ventas.get(clave);
			float acumuladoVendedor = 0;
			for (Float importe : listaVentas)
				acumuladoVendedor += importe;
			resultado.put(clave, acumuladoVendedor);
		}
		return resultado;
	}

	public int calculaEdad(String fechaNacimiento) { // ddmmaaaa
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("ddMMyyyy");
		LocalDate fechaNac = LocalDate.parse(fechaNacimiento, fmt);
		LocalDate ahora = LocalDate.now();
		Period periodo = Period.between(fechaNac, ahora);
		// System.out.printf("Tu edad es: %s años, %s meses y %s días",
		// periodo.getYears(), periodo.getMonths(),
		// periodo.getDays());
		return periodo.getYears();
	}

	// private static String[] diasSemana = { "lunes", "martes", "miercoles",
	// "jueves", "viernes", "sábado", "domingo" };

	public boolean esPrimo(int numero) {

		for (int i = 2; i < numero; i++) {
			if (numero % i == 0)
				return false;
		}

		return true;
	}

	public int[] numerosPrimos(int cuantos) {
		int[] primos = new int[cuantos];
		int i = 0;
		int j = 1;
		while (i < cuantos) {
			if (esPrimo(j))
				primos[i++] = j;
			j++;
		}
		return primos;
	}

	public int[] numerosFibonacci(int cuantos) {
		int[] numeros = new int[cuantos];
		int x = 0;
		int y = 1;
		int z;
		numeros[0] = x;
		numeros[1] = y;
		for (int i = 2; i < cuantos; i++) {
			z = x + y;
			numeros[i] = z;
			x = y;
			y = z;
		}
		return numeros;
	}

	// LIGA: Obtener clasificación a partir de resultados
	public int[] obtenerClasificacion(String[][] goles) {
		int[] puntos = new int[5];
		int golesLocal;
		int golesVisitante;
		String[] resultado = null;
		// recorrer la matriz de goles
		for (int i = 0; i < goles.length; i++)
			for (int j = 0; j < goles[i].length; j++)
				if (goles[i][j].indexOf('-') != -1) {
					resultado = goles[i][j].split("-");
					golesLocal = Integer.parseInt(resultado[0]);
					golesVisitante = Integer.parseInt(resultado[1]);
					if (golesLocal > golesVisitante)
						// suma 3 al local
						puntos[i] += 3;
					else if (golesLocal < golesVisitante)
						// suma 3 al visitante
						puntos[j] += 3;
					else { // empate
						puntos[i] += 1;
						puntos[j] += 1;
					}
				}
		return puntos;
	}

	public int[] obtenerClasificacion2(String[][] goles) {
		// la diferencia con el anterior es que recorre la
		// matriz por columnas
		int[] puntos = new int[5];
		int golesLocal;
		int golesVisitante;
		String[] resultado = null;
		// recorrer la matriz de goles
		for (int j = 0; j < goles[0].length; j++)
			for (int i = 0; i < goles.length; i++)
				if (goles[i][j].indexOf('-') != -1) {
					resultado = goles[i][j].split("-");
					golesLocal = Integer.parseInt(resultado[0]);
					golesVisitante = Integer.parseInt(resultado[1]);
					if (golesLocal > golesVisitante)
						// suma 3 al local
						puntos[i] += 3;
					else if (golesLocal < golesVisitante)
						// suma 3 al visitante
						puntos[j] += 3;
					else { // empate
						puntos[i] += 1;
						puntos[j] += 1;
					}
				}
		return puntos;
	}

	public Equipo[] obtenerClasificacion3(int[][] puntosJornadas) {
		Equipo[] clasificacion = new Equipo[5];
		String[] equipos = new Datos().getEquipos();
		for (int j = 0; j < puntosJornadas[0].length; j++) {
			Equipo e = new Equipo();
			e.setNombre(equipos[j]);
			e.setPuntos(0);
			for (int i = 0; i < clasificacion.length; i++)
				e.setPuntos(e.getPuntos() + puntosJornadas[i][j]);
			clasificacion[j] = e;
		}

		return clasificacion;
	}

	public boolean validarNif(String nif) {
		char[] letrasValidas = { 'T', 'R', 'W', 'A', 'G', 'M', 'Y', 'F', 'P', 'D', 'X', 'B', 'N', 'J', 'Z', 'S', 'Q',
				'V', 'H', 'L', 'C', 'K', 'E' };

		if (nif.length() != 9)
			return false;
		String numeros = nif.substring(0, 8);
		// System.out.println(numeros);
		int numerosOK;
		try {
			numerosOK = Integer.parseInt(numeros);
		} catch (NumberFormatException e) {
			return false;
		}
		int resto = numerosOK % 23;
		if (letrasValidas[resto] != nif.charAt(8))
			return false;
		return true;
	}

	// ORDENACION
	public void ordenaEnteros(int[] numeros) {
		for (int i = 0; i < numeros.length - 1; i++)
			for (int j = i + 1; j < numeros.length; j++)
				if (numeros[i] > numeros[j]) {
					int aux = numeros[i];
					numeros[i] = numeros[j];
					numeros[j] = aux;
				}
	}

	public void ordenaClasificacion(int[] numeros, String[] equipos) {
		for (int i = 0; i < numeros.length - 1; i++)
			for (int j = i + 1; j < numeros.length; j++)
				if (numeros[i] < numeros[j]) {
					int aux = numeros[i];
					numeros[i] = numeros[j];
					numeros[j] = aux;
					String aux2 = equipos[i];
					equipos[i] = equipos[j];
					equipos[j] = aux2;
				}
	}
	// mezcla dos arrays ordenados

	public int[] mezclaArrays(int[] l1, int[] l2) {
		int i = 0, j = 0, k = 0;
		int[] resultado = new int[l1.length + l2.length];

		while (l1[i] != Integer.MAX_VALUE || l2[j] != Integer.MAX_VALUE) {
			if (l1[i] < l2[j])
				resultado[k] = l1[i++];
			else
				resultado[k] = l2[j++];
			k++;

			if (i == l1.length)
				l1[--i] = Integer.MAX_VALUE;

			if (j == l2.length)
				l2[--j] = Integer.MAX_VALUE;
		}
		return resultado;
	}

	public void ordenaCadenas(String[] cadenas) {
		for (int i = 0; i < cadenas.length - 1; i++)
			for (int j = i + 1; j < cadenas.length; j++)
				if (cadenas[i].compareTo(cadenas[j]) > 0) {
					String aux = cadenas[i];
					cadenas[i] = cadenas[j];
					cadenas[j] = aux;
				}

	}

	public void ordenaEstudiantes(Estudiante[] estudiantes) {
		// ejemplo de uso de la interfaz Comparable
		// debe implementarse el método compareTo

		for (int i = 0; i < estudiantes.length - 1; i++)
			for (int j = i + 1; j < estudiantes.length; j++)
				if (estudiantes[i].compareTo(estudiantes[j]) > 0) {
					Estudiante aux = estudiantes[i];
					estudiantes[i] = estudiantes[j];
					estudiantes[j] = aux;
				}
	}

	public float calculaSaldo(float saldoInicial, float[] movimientos) {
		float saldoFinal = saldoInicial;
		for (int i = 0; i < movimientos.length; i++)
			saldoFinal += movimientos[i];
		return saldoFinal;
	}

	public float calculaSaldo(float saldoInicial, ArrayList<Float> movimientos) {
		float saldoFinal = saldoInicial;
		for (Float movimiento : movimientos)
			saldoFinal += movimiento.floatValue();
		return saldoFinal;

	}

	public float calculaSaldo(float saldoInicial, String rutaFicheroMovs) {
		float saldo = saldoInicial;
		try {
			// Abrir el fichero
			FileReader fr = new FileReader("ficheros/personas.txt");

			BufferedReader br = new BufferedReader(fr);
			String linea;
			// System.out.println(LocalDate.now());
			// Leer el fichero linea a linea
			while ((linea = br.readLine()) != null) {

				String[] campos = linea.split("&&");
				System.out.println(linea);
				System.out.println(calculaEdad(campos[2]));

			}
			fr.close();
			br.close();
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}

		return saldo;

	}

	public int[] convierteCadenasANumeros(String[] cadenas) {
		int[] resultado = new int[cadenas.length];
		for (int i = 0; i < resultado.length; i++) {

			try {

				resultado[i] = Integer.parseInt(cadenas[i]);
			} catch (NumberFormatException e) {

				resultado[i] = -1;
			}
		}
		return resultado;
	}

	public ArrayList<Integer> convierteCadenasANumeros(ArrayList<String> cadenas) {
		// int[] resultado = new int[cadenas.length];
		ArrayList<Integer> resultado = new ArrayList<Integer>();
		// for (int i = 0; i < resultado.length; i++) {
		for (String cadena : cadenas) {
			try {

				// resultado[i] = Integer.parseInt(cadenas[i]);
				resultado.add(Integer.parseInt(cadena));
			} catch (NumberFormatException e) {

				// resultado[i] = -1;
				resultado.add(-1);
			}
		}
		return resultado;
	}

	public void muestraNumeros() {

		int x = 0;
		while (x <= 1000) {
			System.out.println("x: " + x);
			x++;
		}
	}

	public void muestraNumeros(int min, int max) {

		if (min <= max) {
			int x = min;
			while (x <= max) {
				System.out.println("x: " + x);
				x++;
			}
		} else
			System.out.println("Error, min debe ser menor que maximo");
	}

	public void muestraNumeros2(int min, int max) {

		if (min <= max) {
			int x = min;
			do {
				System.out.println("x: " + x);
				x++;
			} while (x <= max);
		} else
			System.out.println("Error, min debe ser menor que maximo");
	}

	public void muestraNumeros3(int min, int max) {
		int x = min;
		if (min <= max) {
			// for (int x = min; x < max; x++) {
			// for (;;) {
			while (true) {
				System.out.println("x: " + x);
				x++;
			}
		} else
			System.out.println("Error, min debe ser menor que maximo");
	}

	public void generaAleatorios(int cuantos, int inferior, int superior) // max 30, min 10
	{

		for (int i = 0; i < cuantos; i++)
			System.out.println(inferior + (int) (Math.random() * (superior - inferior + 1)));

	}

	public void generaAleatorios2(int cuantos, int inferior, int superior) // max 30, min 10
	{

		Random rnd = new Random();
		for (int i = 0; i < cuantos; i++)
			System.out.println(inferior + rnd.nextInt(superior - inferior + 1));
	}

	public int[] generaAleatorios3(int cuantos, int inferior, int superior) // max 30, min 10
	{
		int[] resultado = new int[cuantos];
		Random rnd = new Random();
		for (int i = 0; i < cuantos; i++)
			// System.out.println(inferior + rnd.nextInt(superior - inferior + 1));
			resultado[i] = inferior + rnd.nextInt(superior - inferior + 1);
		return resultado;
	}

	public int[] frecuenciaAparicion(int cuantos, int inferior, int superior) {
		int[] resultado = new int[superior - inferior + 1];
		int[] lanzamientos = this.generaAleatorios3(cuantos, inferior, superior);
		for (int i = 0; i < lanzamientos.length; i++) {
			resultado[lanzamientos[i] - 1]++;
		}
		return resultado;

	}

	public void estadisticaCadena(String cadena) {
		int contadorVocales = 0;
		int contadorMayusculas = 0;
		int contadorEspeciales = 0;
		for (int i = 0; i < cadena.length(); i++) {
			if (cadena.charAt(i) == 'a' || cadena.charAt(i) == 'e' || cadena.charAt(i) == 'i' || cadena.charAt(i) == 'o'
					|| cadena.charAt(i) == 'u' || cadena.charAt(i) == 'A' || cadena.charAt(i) == 'E'
					|| cadena.charAt(i) == 'I' || cadena.charAt(i) == 'O' || cadena.charAt(i) == 'U')
				contadorVocales++;
			if (cadena.charAt(i) >= 'A' && cadena.charAt(i) <= 'Z')
				contadorMayusculas++;
			int caracterAscii = cadena.charAt(i);
			if ((caracterAscii >= 0 && caracterAscii <= 47) || (caracterAscii >= 58 && caracterAscii <= 64) ||

					(caracterAscii >= 91) && (caracterAscii <= 96))

				contadorEspeciales++;
		}
		// System.out.println("Hay " + contadorVocales + " vocales en " + cadena);
		System.out.println("Hay " + contadorEspeciales + " caracteres especiales en " + cadena);

	}

	public void listaDiasSemana() {
		Datos datos = new Datos();
		// String[] diasSemana = { "lunes", "martes", "miercoles", "jueves", "viernes",
		// "sábado", "domingo" };
		// for (int i = 0; i < datos.getDiasSemana().length; i++)
		for (String dia : datos.getDiasSemana())
			// System.out.println(datos.getDiasSemana()[i]);
			System.out.println(dia);
	}

	public void listaEstudiantes(Estudiante[] lista) {
		for (Estudiante estudiante : lista) {
			// if (estudiante != null)
			try {
				System.out.println(estudiante.getNombre());
			} catch (NullPointerException e) {

			}
		}
	}

	public void listaEstudiantes(ArrayList<Estudiante> lista) {
		for (Estudiante estudiante : lista) {
			// if (estudiante != null)
			try {
				System.out.println(estudiante.getNombre());
			} catch (NullPointerException e) {

			}
		}
	}

	public int visitantesIslaYear(int isla, int[][] v) {
		int acu = 0;
		for (int j = 0; j < v[0].length; j++)
			acu += v[isla][j];
		return acu;
	}

	public int visitantesMesYear(int mes, int[][] v) {
		int acu = 0;
		for (int i = 0; i < v.length; i++)
			acu += v[i][mes];
		return acu;
	}

	public void recorrerMatrizIrregularPorColumnas(int[][] matriz) {
		int JMAX = 0;
		// obtenemos el numero maximo de columnas
		for (int i = 0; i < matriz.length; i++) {
			if (matriz[i].length > JMAX)
				JMAX = matriz[i].length;
		}

		for (int j = 0; j < JMAX; j++) {
			for (int i = 0; i < matriz.length; i++) {
				try {
					System.out.println("[" + i + "][" + j + "]: " + matriz[i][j]);
				} catch (ArrayIndexOutOfBoundsException e) {
					continue;
				}

			}
		}
	}

	public void recorrerMatrizIrregularPorColumnas2(Integer[][] matriz) {
		int JMAX = 0;
		// obtenemos el numero maximo de columnas
		for (int i = 0; i < matriz.length; i++) {
			if (matriz[i].length > JMAX)
				JMAX = matriz[i].length;
		}

		for (int j = 0; j < JMAX; j++) {
			for (int i = 0; i < matriz.length; i++) {
				try {
					System.out.println("[" + i + "][" + j + "]: " + matriz[i][j].byteValue());
				} catch (ArrayIndexOutOfBoundsException e) {
					continue;
				} catch (NullPointerException e) {
					continue;
				}

			}
		}
	}
}
