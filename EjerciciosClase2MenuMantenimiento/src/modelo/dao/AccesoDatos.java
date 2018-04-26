package modelo.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class AccesoDatos {
	private String usuario;
	private String clave;
	private String host;
	private String bd;

	public AccesoDatos() {
		super();
		// TODO Auto-generated constructor stub
	}

	// TERCERA EVALUACION
	public Connection conexion(String dominio, String bd, String usr, String clave) {
		try {
			String url = "jdbc:mysql://" + dominio + "/" + bd;
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection con = DriverManager.getConnection(url, usr, clave);
			//System.out.println("¡¡Has conectado con la bbdd!!");
			return con;
		} catch (InstantiationException e) {
			System.out.println(e.getMessage());
		} catch (IllegalAccessException e) {
			System.out.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			// System.out.println(e.getMessage());
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		System.out.println("¡¡NO has conectado con la bbdd!!");
		return null;

	}

	public void actualizaTabla(String dominio, String bd, String usr, String clave, String sql) {
		try {
			Connection conexion = this.conexion(dominio, bd, usr, clave);
			// + " where 1=2"
			Statement stm = conexion.createStatement();
			int resultado = stm.executeUpdate(sql);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}

	public ArrayList<HashMap<String, Object>> getAllRecords(String dominio, String bd, String usr, String clave,
			String tabla) {

		try {
			ArrayList<HashMap<String, Object>> registros = new ArrayList<HashMap<String, Object>>();
			Connection conexion = this.conexion(dominio, bd, usr, clave);
			String sql = "SELECT * FROM " + tabla;
			// + " where 1=2"
			Statement stm = conexion.createStatement();
			ResultSet rs = stm.executeQuery(sql);

			ResultSetMetaData metaData = rs.getMetaData();
			rs.first();
			if (rs.getRow() == 0) {
				System.out.println("NO HAY REGISTROS");
				stm.close();
				rs.close();
				return null;
			} else
				rs.beforeFirst();
			while (rs.next()) {

				HashMap<String, Object> registro = new HashMap<String, Object>();
				registros.add(registro);
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					registro.put(metaData.getColumnName(i), rs.getString(i));
					System.out.print(metaData.getColumnName(i) + " => " + rs.getString(i) + "\t");
				}

				System.out.println();
			}

			stm.close();
			rs.close();
			return registros;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	public ArrayList<ArrayList<Object>> getAllRecords2(String dominio, String bd, String usr, String clave,
			String tabla) {

		try {
			ArrayList<ArrayList<Object>> registros = new ArrayList<ArrayList<Object>>();
			Connection conexion = this.conexion(dominio, bd, usr, clave);
			String sql = "SELECT * FROM " + tabla;
			Statement stm = conexion.createStatement();
			ResultSet rs = stm.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			// primera fila: nombres de campos
			ArrayList<Object> registro = new ArrayList<Object>();
			registros.add(registro);
			for (int i = 1; i <= metaData.getColumnCount(); i++)
				registro.add(metaData.getColumnName(i));
			// resto filas: valores de los campos
			while (rs.next()) {
				registro = new ArrayList<Object>();
				registros.add(registro);
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					registro.add(rs.getString(i));
					System.out.print(metaData.getColumnName(i) + " => " + rs.getString(i) + "\t");
				}
				System.out.println();
			}
			stm.close();
			rs.close();
			return registros;
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		return null;
	}
	
	
	
	//MENU MANTENIMIENTO TABLA EJERCICIO
	
	public void menu() {

		String tecleado = "XYZ";
		while (tecleado.compareToIgnoreCase("q") != 0) {
			System.out.println("\t\t ||||||||||||||||||||||||||||||||");
			System.out.println("\t\t ||||||| MENU PRINCIPAL  ||||||||");
			System.out.println("\t\t ||||||||||||||||||||||||||||||||");
			System.out.println("\t\t |||||||||  Opcion 1  |||||||||||");
			System.out.println("\t\t |||||||||  Opcion 2  |||||||||||");
			System.out.println("\t\t |||||||||  Opcion 3  |||||||||||");
			System.out.println("\t\t |||||||||  Opcion 4  |||||||||||");
			System.out.println("\t\t ||||  Press Q or q to EXIT  ||||");
			System.out.println("\t\t ||||||||||||||||||||||||||||||||");
			Scanner teclado = new Scanner(System.in);
			System.out.println("\n\t Selecione una opcion");
			System.out.print("\nOpcion: ");
			tecleado = teclado.nextLine();
			System.out.println("\nUsted ha tecleado la opcion: " + tecleado);
			switch (tecleado) {
			case "1":
				System.out.println("\nEsta opcion se usa para mostrar todos los datos de cualquier\ntabla en una base de datos que seleccionemos");
				showDB();
				//seleccionarBD();
				seleccionarTable();
				opcion1Select();
			case "2":
				System.out.println("Has seleccionado la opcion 2");
				break;
			case "3":
				System.out.println("Has seleccionado la opcion 3");
				break;
			case "4":
				System.out.println("Has seleccionado la opcion 4");
				break;
			default:
				System.out.println("\n Solo números entre 1 y 4");
			}
		}
		System.out.println("Hasta la próxima");
	}
	
	public void showDB() {
		//String bd = "";
		try {
			
			Scanner teclado = new Scanner(System.in);
			Connection conexion = this.conexion("localhost", "", "root", "");        
			String sql = "SHOW DATABASES ";
            Statement stm = conexion.createStatement();
            ResultSet rs = stm.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			System.out.println("\nLISTADO DE BASES DE DATOS: \n");
			while (rs.next()) {

				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					System.out.print(metaData.getColumnName(i) + " => " + rs.getString(i) + "\t");
				}
				System.out.println();
			}
			//System.out.println("Escriba el nombre de alguna de las BD: ");
			//bd = teclado.nextLine();
			stm.close();
			rs.close();
			

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
		//return bd;

	}
	public String seleccionarBD() {
		
		Scanner teclado = new Scanner(System.in);
		System.out.println("\nEscriba el nombre de alguna de las siguientes BD: \n");
		bd = teclado.nextLine();
		//System.out.println(bd);
		return bd;
		

	}
	
	
	public void seleccionarTable() {
		String bd2 = seleccionarBD();
		try {
			
			Scanner teclado = new Scanner(System.in);
			Connection conexion = this.conexion("localhost", bd2, "root", "");        
			String sql = "SHOW TABLES ";
            Statement stm = conexion.createStatement();
            ResultSet rs = stm.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			System.out.println("\nLISTADO DE TABLAS DE LA BD: " + bd2);
			while (rs.next()) {

				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					System.out.print(metaData.getColumnName(i) + " => " + rs.getString(i) + "\t");
				break;
				}
				System.out.println();
			}
			//System.out.println("Escriba el nombre de alguna de las tablas: ");
			//bd = teclado.nextLine();
			stm.close();
			rs.close();
			

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		
		//return bd;

	}
	
	public void opcion1Select() {
		//seleccionarBD();
		//System.out.println("En esta opción podra realizar consultas.");
		System.out.println("\nEscriba el nombre de alguna de las siguientes tablas de la que desea consultar todos sus datos: ");
		try {
			Scanner teclado = new Scanner(System.in);
			Connection conexion = this.conexion("localhost", "paro", "root", "");
	        
			String tabla = teclado.nextLine();
			String sql = "SELECT * FROM " + tabla;
            Statement stm = conexion.createStatement();
            ResultSet rs = stm.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();
			while (rs.next()) {
				System.out.println("tablas: ");
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					System.out.print(metaData.getColumnName(i) + " => " + rs.getString(i) + "\t");
				}
				System.out.println();
			}
			
			stm.close();
			rs.close();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}
	
	
	
	
	
	
	
	
	
	
	//CONSULTAR PADRON COMUNIDAD AUTONOMA
	
	public void consultaPAdronCAProvincias2() {
        try {
            Connection conexion = this.conexion("localhost","paro","root", "");
            String sql = "select CA as Comunidad, provincia, sum(padron) as padron from padron pa inner join provincias p1, comunidadesautonomas c1, municipios m1 where pa.CodMunicipio \r\n" + 
                    "= m1.CodMunicipio and m1.CodProvincia = p1.CodProvincia and p1.CodCA = c1.CodCA group by p1.CodProvincia order by c1.CA, p1.Provincia";
            Statement stm = conexion.createStatement();
            ResultSet rs = stm.executeQuery(sql);
            
            int subtotal =0;
            int subtotal2 = 0;
            int total = 0;
            String ca_ant =null;
           
            while (rs.next()) {
                if(rs.getString(1).equals(ca_ant) == false) {
                    if(subtotal !=0) {
                        System.out.println("\n subtotal: " + subtotal);
                    }
                    System.out.println("\n COMUNIDAD AUTONOMA : " + rs.getString(1));
                    ca_ant = rs.getString(1);
                    total+=subtotal;
                    subtotal = 0;
                    
                }
                subtotal += rs.getInt(3);
                
                System.out.println("\t" + rs.getString(2) + ": " + rs.getInt(3));
                
                
                
            }
            System.out.println("\n total: " + total);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

	

	public void consultaPadronCAProvinciasMau() {

		try {
			Connection conexion = this.conexion("localhost", "paro", "root", "");
			String sql = "select CA as Comunidad, provincia, sum(padron) as padron from padron pa inner join provincias p1, comunidadesautonomas c1, municipios m1 "
					+ " where pa.CodMunicipio = m1.CodMunicipio and m1.CodProvincia = p1.CodProvincia and p1.CodCA = c1.CodCA group by p1.CodProvincia order by c1.CA, p1.Provincia";
			Statement stm = conexion.createStatement();
			ResultSet rs = stm.executeQuery(sql);
			int subtotal = 0, total = 0;
			String ca_ant = "";
			int i = 0;
			while (rs.next()) {
				
				if (!rs.getString(1).equals(ca_ant) ) {
					if (subtotal!=0){
						System.out.println(" TOTAL COMUNIDAD AUTONOMA : " + ca_ant  + ", " + subtotal);
					}
					//&& !ca_ant.equals("")
					System.out.println("COMUNIDAD AUTONOMA : " + rs.getString(1));
					total += subtotal;
					
					
					subtotal = 0;
					ca_ant= rs.getString(1);
				}
				
					
				System.out.println("\t\t" +  rs.getString(2) + " = " + rs.getInt(3) );
				subtotal += rs.getInt(3);

			}
			System.out.println("TOTAL ESPAÑA : " + total);
			rs.close();
			stm.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
}
