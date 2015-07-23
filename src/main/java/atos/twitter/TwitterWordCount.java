package atos.twitter;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TwitterWordCount {

	public static final String tweetTextCountId = "tweet-text-count";
	public static final String invalidTweetCountId = "invalid-tweet-count";

	public static void main(String[] args) throws Exception{

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> tweetTextDataset;

		/*Read directly from compressed file*/
/*		DataSet<Tuple2<LongWritable,Text>> compressedDataSet = readCompressedXZ(env,"/home/tamara/Documents/atos/data.tar.xz");
		tweetTextDataset = compressedDataSet.map(new ExtractCompressedString()).flatMap(new ParseLineToTweet());
*/
		/*Read dt1_raw.csw into a DataSet of Strings using readTextFile*/
/*		DataSet<String> dataSet = env.readTextFile("/home/tamara/Documents/atos/dt1_raw.csv");
		tweetTextDataset = dataSet.flatMap(new ParseLineToTweet());
*/
		/*Read dt1_raw.csv into a Tuple of Strings*/
		DataSet<Tuple5<String,String,String,String,String>> tupleDataSet =
				env.readCsvFile("/home/tamara/Documents/atos/dt1_raw.csv").ignoreFirstLine()
					.ignoreInvalidLines().parseQuotedStrings('"')
					.types(String.class, String.class, String.class, String.class, String.class);
		tweetTextDataset = tupleDataSet.flatMap(new TweetFromTuple()).map(new TextFromTweet());

		/*Read dt1_raw.csv into a Tweet POJO*/
/*		DataSet<SummarizedTweet> pojoDataSet =
				env.readCsvFile("/home/tamara/Documents/atos/dt1_raw.csv").ignoreFirstLine()
					.ignoreInvalidLines().parseQuotedStrings('\"')
					.pojoType(SummarizedTweet.class, "id", "text", "date", "users", "sentiment");
		tweetTextDataset = pojoDataSet.map(new TextFromTweet());*/

		/*Transformations to Tweet Text*/
		tweetTextDataset.map(new LineCleaner()).flatMap(new Tokenizer()).filter(new RemoveStopWords())
						.groupBy(0).sum(1).filter(new MinimumWordCount(20)).print();

		//env.execute("Twitter word count");
		JobExecutionResult result = env.getLastJobExecutionResult();
		System.out.println(tweetTextCountId + " :" + result.getAccumulatorResult(tweetTextCountId));
		System.out.println(invalidTweetCountId + " :" + result.getAccumulatorResult(invalidTweetCountId));
	}

	/*************************************************
	 UDFS
	 *************************************************/
	public static final class LineCleaner implements MapFunction<String, String> {

		@Override
		public String map(String value) {
			// normalize and split the line into words
			String normalized = Normalizer.normalize(value, Normalizer.Form.NFD);

			String text = normalized.replaceAll("[^\\p{ASCII}]", "").toLowerCase();
			// Elimamos los usuarios de twitter
			text = text.replaceAll("@[a-z0-9-_]+(\\s|:|.)","");
			// Eliminamos las urls
			text = text.replaceAll("(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]","");
			// Elimnamos la palabra RT
			text = text.replaceAll("(rt :|rt:|rt )","");
			// Elimnamos los signos de puntacion sueltos
			text = text.replaceAll("\\p{Punct}","");
			// Elimnamos los digitos sueltos
			text = text.replaceAll("\\p{Digit}","");
			// Elimnamos las letras sueltas
			text = text.replaceAll(" [a-z] "," ");
			// Eliminamos los signos extraños
			text = text.replaceAll(" [^a-zA-Z] "," ");

			return text;
		}
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	public static final class RemoveStopWords implements FilterFunction<Tuple2<String, Integer>> {

		public boolean filter(Tuple2<String, Integer> value) throws Exception {
			List<String> stopwords = Arrays.asList("un", "una", "unas", "unos", "uno", "sobre", "todo", "también", "tras", "otro", "otra", "algún", "alguno", "alguna", "algunos", "algunas", "ser", "es", "soy", "eres", "somos", "sois", "estoy", "esta", "estamos", "estais", "estan", "como", "en", "para", "atras", "porque", "por qué", "estaba", "ante", "antes", "siendo", "ambos", "pero", "por", "poder", "puede", "puedo", "podeis", "pueden", "fui", "fue", "fuimos", "fueron", "hacer", "hago", "hace", "hacemos", "haceis", "hacen", "cada", "fin", "incluso", "primero", "desde", "conseguir", "consigo", "consigue", "consigues", "conseguimos", "consiguen", "ir", "voy", "va", "vamos", "vais", "van", "vaya", "gueno", "ha", "tener", "tengo", "tiene", "tenemos", "teneis", "tienen", "el", "la", "lo", "las", "los", "su", "aqui", "alli", "mio", "tu", "tú", "tuyo", "ellos", "ellas", "nos", "nosotros", "vosotros", "vosotras", "si", "dentro", "solo", "solamente", "saber", "sabes", "sabe", "sabemos", "sabeis", "saben", "ultimo", "largo", "bastante", "haces", "muchos", "aquellos", "aquellas", "sus", "entonces", "verdadero", "verdadera", "cierto", "ciertos", "cierta", "ciertas", "intentar", "intento", "intenta", "intentas", "intentamos", "intentais", "intentan", "dos", "bajo", "arriba", "encima", "usar", "uso", "usas", "usa", "usamos", "usais", "usan", "emplear", "empleas", "emplean", "ampleamos", "empleais", "valor", "muy", "era", "eras", "eramos", "eran", "modo", "bien", "cual", "cuando", "donde", "mientras", "quien", "quién", "con", "entre", "sin", "trabajo", "trabajar", "trabajas", "trabaja", "trabajamos", "trabajais", "trabajan", "podria", "podrias", "podriamos", "podrian", "podriais", "yo", "aquel", "y", "http", "que", "de", "del", "q", "se", "ni", "son", "he", "ya", "vuestra", "vuestro", "vuestras", "vuestros", "esto", "este", "esta", "estos", "estas", "eso", "esa", "esos", "esas", "han", "al", "más", "no", "sí", "ella", "el", "le", "les", "me", "mi", "te", "tus", "qué", "hoy", "todos", "todas", "todo", "toda", "muchas", "muchos", "mucha", "mucho", "ahora", "hoy", "ayer", "mañana", "mañanas", "mes", "meses", "dia", "dias", "día", "semana", "semanas", "año", "años", "nueva", "nuevas", "sea", "tuits", "rt", "ht", "deja", "os", "muchísimas", "muchísimos", "muchísima", "muchísimo", "pronto", "tarde", "vemos", "tenéis", "teneis", "eliminará", "eliminara", "hemos", "casi", "sonará", "buenos", "bueno", "buenas", "buena", "tanta", "tanto", "está", "están", "parece", "otros", "otras", "aquí", "allí", "hay", "algo", "»", "\"", "/", "#", "¦", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "ñ", "o", "p", "q", "r", "s", "t", "u", "w", "x", "y", "z", ".", ",", ":", "¿", "?", "!", "¡", "'", "\"", "_", "-", "+", "(", ")", "…", "«", "‘", "’");
			return !stopwords.contains(value.f0);
		}
	}

	public static final class MinimumWordCount implements FilterFunction<Tuple2<String,Integer>>{

		private int minimumCount;

		public MinimumWordCount(int minimumCount){
			this.minimumCount = minimumCount;
		}

		@Override
		public boolean filter(Tuple2<String,Integer> value){
			return value.f1>=minimumCount;
		}
	}

	public static final class ParseLineToTweet implements FlatMapFunction<String,String>{

		@Override
		public void flatMap(String value, Collector<String> out){
			Pattern regex = Pattern.compile("(\\\")(.*?)(\\\",)");
			Matcher m = regex.matcher(value);
			System.out.println(m.groupCount());
			int i=0;
			while (m.find()) {
				System.out.println(m.group(i++));
			}
			out.collect("tweet");
		}

	}

	/*************************************************
	 Tweet Functions
	 *************************************************/

	public static final class TweetFromTuple extends RichFlatMapFunction<Tuple5<String,String,String,String,String>,SummarizedTweet> {

		LongCounter validTweetAccumulator;
		IntCounter invalidLineAccumulator;

		@Override
		public void open(Configuration parameters){
			validTweetAccumulator = getRuntimeContext().getLongCounter(tweetTextCountId);
			invalidLineAccumulator = getRuntimeContext().getIntCounter(invalidTweetCountId);
		}

		@Override
		public void flatMap(Tuple5<String,String,String,String,String> value, Collector<SummarizedTweet> out){
			SummarizedTweet tweet = new SummarizedTweet();
			try {
				tweet.setId(Integer.parseInt(value.f0));
				tweet.setText(value.f1);
				tweet.setDate(value.f2);
				tweet.setUsers(Long.parseLong(value.f3));
				tweet.setSentiment(Integer.parseInt(value.f4));
				validTweetAccumulator.add(1L);
				out.collect(tweet);
			} catch (NumberFormatException ex){
				invalidLineAccumulator.add(1);
			}
		}
	}

	public static final class TextFromTweet implements MapFunction<SummarizedTweet, String> {

		@Override
		public String map(SummarizedTweet tweet) throws Exception {
			return tweet.getText();
		}
	}

	/*************************************************
	 Data Compression Functions
	 *************************************************/

	public static DataSet<Tuple2<LongWritable, Text>> readCompressedXZ(ExecutionEnvironment env, String inputPath){

		JobConf configuration = new JobConf();
		configuration.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");
		HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<LongWritable, Text>(new TextInputFormat(), LongWritable.class, Text.class, configuration);
		TextInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new org.apache.hadoop.fs.Path(inputPath));
		return env.createInput(hadoopInputFormat);
	}

	public static final class  ExtractCompressedString implements MapFunction<Tuple2<LongWritable,Text>,String>{

		@Override
		public String map(Tuple2<LongWritable,Text> value){
			return value.f1.toString();
		}
	}

}


