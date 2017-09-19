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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
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

public class Log {

	public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(int[] v) {
            super(IntWritable.class);
            IntWritable[] nuevo = new IntWritable[v.length];
            for (int i = 0; i < v.length; i++) {
                nuevo[i] = new IntWritable(v[i]);
            }
            set( nuevo );
        }

		public int[] toArray(){
			IntWritable[] aux = (IntWritable[]) super.toArray();
			int[] aux2 = new int[aux.length];
			for (int i = 0; i < aux2.length; i++) {
				aux2[i] = aux[i].get();
			}
		
			return aux2;
		}

		public String toString(){
			String s = "(";
			for (Writable writable: get()) {                 
				IntWritable intWritable = (IntWritable)writable;  
				int value = intWritable.get();                    
				s += value + ", ";
			}
			if (s.endsWith(", ")) {
				s = s.substring(0, s.length() - 2);
			}
			s += ")";
			return s;
		}
    }


  /**
   * <p>
   * El mapper extiende de la interfaz org.apache.hadoop.mapreduce.Mapper. Cuando
   * se ejecuta Hadoop el mapper recibe cada linea del archivo de entrada como
   * argumento. La funcion "map" parte cada linea y para cada palabra emite la
   * pareja (word, [1, numBytes, error]) como salida.</p>
   */
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntArrayWritable>{
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] split = (value.toString()).split(" ");

		// columnas del fichero de entrada
		int posHost = 0, posFecha = 1, posVerbo = 2, posCodigoHTTP = split.length-2, posNumBytes = split.length-1;

		// formamos el valor de salida (codigoHTTP, numeroBytes)
		int[] resultado = new int[3];

		// una peticion
		resultado[0] = 1;
		
		// si hay '-' es 0
		if(split[posNumBytes].equals("-")) resultado[1] = 0;
		else resultado[1] = Integer.parseInt(split[posNumBytes]);

		// si no hay error es 0
		if(split[posCodigoHTTP].startsWith("5") || split[posCodigoHTTP].startsWith("4")) resultado[2] = 1;
		else resultado[2] = 0;

		// devolvemos (host,(codigoHTTP, numeroBytes))
		word.set( split[posHost] );
    	context.write(word, new IntArrayWritable(resultado));
		
    }
  }

	public static class LogCombiner
		  extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
    private IntArrayWritable result = new IntArrayWritable();

    public void reduce(Text key, Iterable<IntArrayWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
			
			int peticiones = 0, numBytes = 0, numCod = 0;

			int[] aux;

			for(IntArrayWritable val : values){

				aux = val.toArray();

				peticiones = peticiones + aux[0];
				numBytes = numBytes + aux[1];
				numCod = numCod + aux[2];

			}
			 
			// lo devolvemos
     		context.write(key, new IntArrayWritable( new int[] {peticiones, numBytes, numCod} ));
		}
    }
	
	
  
  /**
   * <p>La funcion "reduce" recibe los valores (apariciones) asociados a la misma
   * clave (palabra) como entrada.
   */
  public static class IntSumReducer 
       extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
    private IntArrayWritable result = new IntArrayWritable();

    public void reduce(Text key, Iterable<IntArrayWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
			
			int peticiones = 0, numBytes = 0, numCod = 0;

			int[] aux;

			for(IntArrayWritable val : values){

				aux = val.toArray();

				peticiones = peticiones + aux[0];
				numBytes = numBytes + aux[1];
				numCod = numCod + aux[2];

			}
			 
			// lo devolvemos
     		context.write(key, new IntArrayWritable( new int[] {peticiones, numBytes, numCod} ));
		
    }
  }

  /**
   * <p>Clase principal con metodo main que iniciara la ejecucion de la tarea</p>
   */
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(Log.class);
    job.setMapperClass(TokenizerMapper.class);
    // Combinador
    job.setCombinerClass(LogCombiner.class);
    job.setReducerClass(IntSumReducer.class);

    // Declaracion de tipos de salida para el mapper
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntArrayWritable.class);
    // Declaracion de tipos de salida para el reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntArrayWritable.class);

    // Archivos de entrada y directorio de salida
	FileInputFormat.addInputPath(job, new Path( "weblog.txt" ));
    FileOutputFormat.setOutputPath(job, new Path( "salida" ));
    
    // Aqui podemos elegir el numero de nodos Reduce
    // Dejamos 1 para que toda la salida se guarde en el mismo fichero 'part-r-00000'
    job.setNumReduceTasks(1);

		// Ejecuta la tarea y espera a que termine. El argumento boolean es para 
    // indicar si se quiere informacion sobre de progreso (verbosity)
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

