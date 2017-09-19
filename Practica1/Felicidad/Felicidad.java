
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* Asignatura: SGDI, Practica 1 
* Realizada por Jose Javier Martinez Pages y Cristina Valentina Espinosa Victoria
* Este documento es fruto exclusivamente del trabajo de sus miembros
*/

public class Felicidad {

  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop el mapper recibe cada linea del archivo de entrada como
   * argumento. La funcion "map" parte cada linea y emite la
   * pareja (triste,word) como salida.</p>
   */
  
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private final static FloatWritable temp = new FloatWritable();
    private Text triste = new Text("triste");
	private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
			
	  //Separamos por tabulador
      String[] datos = (value.toString()).split("\t");
	  //Posiciones de la palabra, felicidad media y raking de twitter
	  int pos_word = 0, pos_media = 2, pos_rank = 4;

		
		//Si la media es menor que dos y tiene ranking de twitter, emitimos la palabra
		Float media = Float.parseFloat(datos[pos_media]);
		if(media < 2 && !datos[pos_rank].equals("--")){
			word.set(datos[pos_word]);
			context.write(triste, word);
		}
      }
   }
 
  
  /**
   * <p>La funcion "reduce" recibe los valores (apariciones) asociados a la misma
   * clave (palabra) como entrada y produce una pareja con la palabra y el numero
   * total de palabras tristes.</p>  
   */

  
  public static class IntSumReducer 
       extends Reducer<Text,Text,IntWritable,Text> {
	   
    private Text result = new Text();	
	private IntWritable total = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		  
		int cont = 0;
		String palabras = "";
		//Contamos el numero de palabras total y guardamos las palabras en un string
		for (Text val : values) {
			cont = cont + 1;
			palabras = palabras + val + " ";
		}
		total.set(cont);		
		result.set(palabras);
		context.write(total, result);
	}
  }

  /**
   * <p>Clase principal con metodo main que iniciara la ejecucion de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Felicidad.class);
    job.setMapperClass(TokenizerMapper.class);
    //Si existe combinador
    //job.setCombinerClass(Clase_del_combinador.class);
    job.setReducerClass(IntSumReducer.class);

    // Declaracion de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // Declaracion de tipos de salida para el reducer
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    // Archivos de entrada y directorio de salida
    FileInputFormat.addInputPath(job, new Path( "happiness.txt" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aqui podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere informacion sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

