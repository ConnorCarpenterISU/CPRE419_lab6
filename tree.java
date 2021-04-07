import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class tree {

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
		JavaRDD<String> pat = context.textFile(args[0]);
		JavaRDD<String> patents = pat.filter(x -> !x.isEmpty());

		JavaPairRDD<String, String> edges = patents.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			public Iterator<Tuple2<String, String>> call(String s) {

				String start = s.split("\\s+")[0];
				String end = s.split("\\s+")[1];

				return Arrays.asList(new Tuple2<String, String>(start, end), new Tuple2<String, String>(end, start))
						.iterator();
			}
		});

		JavaPairRDD<String, Iterable<String>> triads = edges.groupByKey();

		JavaPairRDD<Tuple2<String, String>, String> possibleTriads = triads
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, // input
						Tuple2<String, String>, // key (output)
						String // value (output)
				>() {
					public Iterator<Tuple2<Tuple2<String, String>, String>> call(Tuple2<String, Iterable<String>> s) {

						// s._1 = String (as a key)
						// s._2 = Iterable<String> (as a values)
						Iterable<String> values = s._2;
						// we assume that no node has an ID of zero
						List<Tuple2<Tuple2<String, String>, String>> result = new ArrayList<Tuple2<Tuple2<String, String>, String>>();

						// Generate possible triads.
						for (String value : values) {
							Tuple2<String, String> k2 = new Tuple2<String, String>(s._1, value);
							Tuple2<Tuple2<String, String>, String> k2v2 = new Tuple2<Tuple2<String, String>, String>(k2,
									"");
							result.add(k2v2);
						}

						// RDD's values are immutable, so we have to copy the values
						// copy values to valuesCopy
						List<String> valuesCopy = new ArrayList<String>();
						for (String item : values) {
							valuesCopy.add(item);
						}
						Collections.sort(valuesCopy);

						// Generate possible triads.
						for (int i = 0; i < valuesCopy.size() - 1; ++i) {
							for (int j = i + 1; j < valuesCopy.size(); ++j) {
								Tuple2<String, String> k2 = new Tuple2<String, String>(valuesCopy.get(i),
										valuesCopy.get(j));
								Tuple2<Tuple2<String, String>, String> k2v2 = new Tuple2<Tuple2<String, String>, String>(
										k2, s._1);
								result.add(k2v2);
							}
						}

						return result.iterator();
					}
				});

		JavaPairRDD<Tuple2<String, String>, Iterable<String>> triadsGrouped = possibleTriads.groupByKey();

		JavaRDD<Tuple3<String, String, String>> trianglesWithDuplicates = triadsGrouped
				.flatMap(new FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<String>>, // input
						Tuple3<String, String, String> // output
				>() {
					@Override
					public Iterator<Tuple3<String, String, String>> call(
							Tuple2<Tuple2<String, String>, Iterable<String>> s) {

						// s._1 = Tuple2<String,String> (as a key) = "<nodeA><,><nodeB>"
						// s._2 = Iterable<String> (as a values) = {0, n1, n2, n3, ...} or {n1, n2, n3,
						// ...}
						// note that 0 is a fake node, which does not exist
						Tuple2<String, String> key = s._1;
						Iterable<String> values = s._2;
						// we assume that no node has an ID of zero

						List<String> list = new ArrayList<String>();
						boolean haveSeenSpecialNodeZero = false;
						for (String node : values) {
							if (node.equals("")) {
								haveSeenSpecialNodeZero = true;
							} else {
								list.add(node);
							}
						}

						List<Tuple3<String, String, String>> result = new ArrayList<Tuple3<String, String, String>>();
						if (haveSeenSpecialNodeZero) {
							if (list.isEmpty()) {
								// no triangles found
								// return null;
								return result.iterator();
							}
							// emit triangles
							for (String node : list) {
								String[] aTraingle = { key._1, key._2, node };
								Arrays.sort(aTraingle);
								Tuple3<String, String, String> t3 = new Tuple3<String, String, String>(aTraingle[0],
										aTraingle[1], aTraingle[2]);
								result.add(t3);
							}
						} else {
							// no triangles found
							// return null;
							return result.iterator();
						}

						return result.iterator();
					}
				});

		JavaRDD<Tuple3<String, String, String>> uniqueTriangles = trianglesWithDuplicates.distinct();
							
		System.out.println("=== Unique Triangles ===");
		List<Tuple3<String, String, String>> output = uniqueTriangles.collect();
		for (Tuple3<String, String, String> t3 : output) {
			// System.out.println(t3._1 + "," + t3._2+ "," + t3._3);
			System.out.println("t3=" + t3);
		}
		
		JavaPairRDD<String, Integer> ones = uniqueTriangles.mapToPair(new PairFunction<Tuple3<String, String, String>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple3<String, String, String> t) {

				return new Tuple2<String, Integer>("triangle", 1);
			}
		});

		JavaPairRDD<String, Integer> count = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);
		
		JavaRDD<Integer> submit = count.map(new Function<Tuple2<String, Integer>, Integer>() {
			public Integer call(Tuple2<String, Integer> t) {
				return t._2;
			}
		});

		submit.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}