import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* Asignatura: SGDI, Practica 1 
* Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
* Este documento es fruto exclusivamente del trabajo de sus miembros
*/

public class IndiceInvertido {

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop el mapper recibe cada linea del archivo de entrada como
   * argumento. La funcion "map" parte cada linea y para cada palabra emite la
   * pareja (word, fileName) como salida.</p>
   */
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

		ArrayList<String> lista = new ArrayList<String>();

		// recorremos las palabras y eliminamos lo que no sean letras
      while (itr.hasMoreTokens()) {
        String word_aux = itr.nextToken();

		word_aux = word_aux.replaceAll("[^a-zA-Z]", "").toLowerCase();
		if(word_aux.matches("[a-zA-Z]+")){
			lista.add(word_aux);
		}

        
      }

	/* Para conseguir el nombre del fichero. */

		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//String fileName = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir", null);


		// devolvemos cada palabra con el fichero en el que aparece
		for(String palabra : lista) {
			word.set( palabra );
        	context.write(word, new Text(fileName));
		}

    }
  }
  
  /**
   * <p>La funcion "reduce" recibe los valores (apariciones) asociados a la misma
   * clave (palabra) como entrada y produce una pareja 
   */
  public static class IntSumReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
	
		
		// contamos el numero de veces que aparece la palabra en un fichero
		// guardamos los datos en una tabla hash con clave = fichero y valor = numero de apariciones
		boolean hayMasDeVeinte = false;		

		Map<String, Integer> diferentes = new HashMap<String, Integer>();
		for (Text value : values) {
	        String word_aux = value.toString();
			Integer veces = diferentes.get(word_aux);
			if(veces == null){
				veces = 0;
			}
			else if(veces == 19){
				hayMasDeVeinte = true;
			}
			diferentes.put(word_aux, veces+1);
		}

		// si la palabra aparece mas de veinte veces la incluimos y la devolvemos,
		// y ademas hay que ordenar segun el numero de apariciones
		String resultado = "";
		if(hayMasDeVeinte){

			//movemos los datos del arbol a arrays
			String claves[] = new String[3];
			Integer valores[] = new Integer[3];
			int i = 0;
			for(Map.Entry<String, Integer> entry : diferentes.entrySet()){

				claves[i] = entry.getKey();
				valores[i] = entry.getValue();
					
				i++;
			}

			// buscamos cual es el mayor y el segundo mayor
			int primero = 0, segundo = 0;
			for (int j = 1; j < i; j++){
			   int nuevo = valores[j];
			   if(nuevo > valores[primero]){
					segundo = primero;
			  		 primero = j;
			  	}
				else if(nuevo > valores[segundo]){
					segundo = j;
				}
				else if(j==1) segundo = j;
			}

			// formamos el resultado
			resultado += "("+claves[primero].toString()+", "+valores[primero].toString()+"), ";
			if(i > 1) resultado += "("+claves[segundo].toString()+", "+valores[segundo].toString()+"), ";
			if(i > 2) resultado += "("+claves[3-(primero+segundo)].toString()+", "+valores[3-(primero+segundo)].toString() + ")";
			 
			// lo devolvemos
			result.set(resultado);
     		context.write(key, result);
		}
    }
  }

  /**
   * <p>Clase principal con metodo main que iniciara la ejecucion de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(IndiceInvertido.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    //job.setCombinerClass(Clase_del_combinador.class);
    job.setReducerClass(IntSumReducer.class);

    // Declaracion de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // Declaracion de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Archivos de entrada y directorio de salida
	 FileInputFormat.addInputPath(job, new Path( "Adventures_of_Huckleberry_Finn.txt" ));
	 FileInputFormat.addInputPath(job, new Path( "Hamlet.txt" ));
	 FileInputFormat.addInputPath(job, new Path( "Moby_Dick.txt" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aqui podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere informacion sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

