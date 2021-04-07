import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class github {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: WordCount <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Firewall in Spark");
		// .setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> github = context.textFile(args[0]);

		JavaRDD<String> github_map = github.map(new Function<String, String>() {
			public String call(String s) {

				String[] lineArray = s.split(",");
				String repo = lineArray[0];
				String lang = lineArray[1];
//				String arch = s.split(",")[2];
//				String comm = s.split(",")[3];
//				String cont = s.split("\\,")[4];
//				String doc = s.split("\\,")[5];
//				String history = s.split("\\,")[6];
//				String license = s.split("\\,")[7];
//				String manage = s.split("\\,")[8];
//				String size = s.split("\\,")[9];
//				String unit_test = s.split("\\,")[10];
//				String state = s.split("\\,")[11];
				String stars = lineArray[12];

				return lang + " " + repo + " " + stars;
			}
		});

		JavaPairRDD<String, Tuple2<String, Integer>> lang_stars = github_map
				.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
					public Tuple2<String, Tuple2<String, Integer>> call(String s) {

						String lang = s.split("\\s+")[0];
						String remainder = s.split("\\s+", 2)[1];

						Tuple2<String, Integer> stars = new Tuple2<String, Integer>(remainder.split("\\s+", 2)[0],
								Integer.parseInt(remainder.split("\\s+", 2)[1]));
						return new Tuple2<String, Tuple2<String, Integer>>(lang, stars);
					}
				});

		JavaPairRDD<String, Tuple2<String, Integer>> max_stars = lang_stars.reduceByKey(
				new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
						Integer max = Math.max(t1._2, t2._2);
						String repo;
						if (t1._2 == max) {
							repo = t1._1;
						} else if (t2._2 == max) {
							repo = t2._1;
						}
						return new Tuple2<String, Integer>("repo", max);
					}
				}, numOfReducers);

		// JavaPairRDD<key_type, output_type> var2 = var1.reduceByKey(new
		// Function2<value_type, value_type, output_type>() {
		// public output_type call(value_type i1, value_type i2) {
		// return ......
		// }
		// }, numOfReducers);

		JavaPairRDD<String, Integer> ones = github_map.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {

				String lang = s.split("\\s+")[0];

				return new Tuple2<String, Integer>(lang, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);

		JavaPairRDD<Integer, String> swap = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
						return t.swap();
					}
				});

		JavaPairRDD<Integer, String> star_test = github_map.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {

				String lang = s.split("\\s+")[0];
				String repo = s.split("\\s+")[1];
				Integer stars = Integer.parseInt(s.split("\\s+")[2]);

				return new Tuple2<Integer, String>(stars, lang + " " + repo);
			}
		});

		JavaPairRDD<Integer, String> max_star_test = star_test.sortByKey(false);

		JavaPairRDD<Integer, String> num_of_repo = swap.sortByKey(false);

		// JavaPairRDD<key_type, value_type> var2 = var1.mapToPair(new
		// PairFunction<input_type, key_type, value_type>() {
		// public Tuple2<key_type, value_type> call(input_type s) {
		// ......
		// return new Tuple2<key_type, value_type>(..., ...);
		// }
		// });

		JavaRDD<String> star_map = max_stars.map(new Function<Tuple2<String, Tuple2<String, Integer>>, String>() {
			public String call(Tuple2<String, Tuple2<String, Integer>> t) {

				String lang = t._1;
				String repo = t._2._1;
				Integer stars = t._2._2;

				return lang + " " + repo + " " + stars;
			}
		});

		JavaPairRDD<String, Tuple2<Integer, Tuple2<String, Integer>>> join = counts.join(max_stars);

		JavaPairRDD<Integer, String> join_map = join.mapToPair(
				new PairFunction<Tuple2<String, Tuple2<Integer, Tuple2<String, Integer>>>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Tuple2<Integer, Tuple2<String, Integer>>> t) {

						String lang = t._1;
						Integer count = t._2._1;
						String repo = t._2._2._1;
						Integer max_stars = t._2._2._2;

						return new Tuple2<Integer, String>(count, lang + " " + repo + " " + String.valueOf(max_stars));
					}
				});

		JavaPairRDD<Integer, String> repo_ord = join_map.sortByKey(false);

		JavaRDD<String> format = repo_ord.map(new Function<Tuple2<Integer, String>, String>() {
			public String call(Tuple2<Integer, String> t) {

				Integer count = t._1;
				String lang = t._2.split("\\s+")[0];
//				String repo = t._2.split("\\s+")[1];
				String max_stars = t._2.split("\\s+")[2];

				return lang + " " + String.valueOf(count) + " " + max_stars;
			}
		});

		JavaPairRDD<Integer, String> repo_tup = format.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {

				String lang = s.split("\\s+")[0];
				String count = s.split("\\s+")[1];
				Integer stars = Integer.parseInt(s.split("\\s+")[2]);

				return new Tuple2<Integer, String>(stars, lang + " " + count);
			}
		});

		JavaPairRDD<Integer, Tuple2<String, String>> final_join = repo_tup.join(max_star_test);

		JavaRDD<String> format_final = final_join.map(new Function<Tuple2<Integer, Tuple2<String, String>>, String>() {
			public String call(Tuple2<Integer, Tuple2<String, String>> t) {

				Integer max_stars = t._1;
				String lang = t._2._1.split("\\s+")[0];
				String repo = t._2._2.split("\\s+")[1];
				String count = t._2._1.split("\\s+")[1];

				return lang + " " + count + " " + repo + " " + max_stars;
			}
		});

		format_final.saveAsTextFile(args[1]);
		context.stop();
		context.close();

	}

}
