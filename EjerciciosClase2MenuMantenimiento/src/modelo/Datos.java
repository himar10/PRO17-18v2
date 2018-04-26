package modelo;

public class Datos {
	private  String[] diasSemana = { "lunes", "martes", "miercoles", "jueves", "viernes", "sábado", "domingo" };
    private Estudiante[] estudiantes;
    private   int numEstudiantes = 30;
    
    // ejemplo de la liga
    private String[] equipos = {"RMA","BCN","VAL","SEV","UDLP"};
    private String[][] resultados = {
    		{"","3-2","1-1","2-0","3-3"}, // RMA LOCAL
    		{"2-2","","3-1","2-1","1-2"}, // BCN LOCAL
    		{"1-1","1-2","","2-1","1-1"}, // VAL LOCAL
    		{"2-2","1-3","1-0","","3-0"}, // SEV LOCAL
    		{"0-0","1-2","2-1","2-3",""}, // UDLP LOCAL 		
    };
    private	int[][] puntosJornada= {
    			// primera vuelta
    			{3,3,1,3,1},	
    			{1,1,1,0,0},
    			{3,0,1,3,1},
    			{3,1,1,0,1},
    			//  segunda vuelta
    			{3,3,1,3,1},	
    			{1,1,1,0,0},
    			{3,0,1,3,1},
    			{3,1,1,0,1}
    	};
    
	// Declarar propiedad private,un array  de Estudiantes
	// crea getter/setter
	// en el constructor de esta clase, INICIALIZAR dicho array 
	
	public Datos() {
		
	// INICIALIZA AQUI EL ARRAY estudiantes....
	estudiantes = new Estudiante[numEstudiantes];
	Estudiante est1 = new Estudiante(12);
	est1.setNombre("Pedro Perez");
	Estudiante est2 = new Estudiante(12);
	est2.setNombre("Maria Navarro");
	
	
	estudiantes[0]= est1;
	estudiantes[1]= est2;

	}

	public int[][] getPuntosJornada() {
		return puntosJornada;
	}

	public void setPuntosJornada(int[][] puntosJornada) {
		this.puntosJornada = puntosJornada;
	}

	public String[][] getResultados() {
		return resultados;
	}

	public String[] getEquipos() {
		return equipos;
	}

	public void setEquipos(String[] equipos) {
		this.equipos = equipos;
	}

	public Estudiante[] getEstudiantes() {
		return this.estudiantes;
	}

	public void setEstudiantes(Estudiante[] estudiantes) {
		this.estudiantes = estudiantes;
	}

	public  String[] getDiasSemana() {
		return diasSemana;
	}

	public  void setDiasSemana(String[] diasSemana) {
		diasSemana = diasSemana;
	}

}
