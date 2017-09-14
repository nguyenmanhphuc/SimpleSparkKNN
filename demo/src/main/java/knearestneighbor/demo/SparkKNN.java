package knearestneighbor.demo;

import org.apache.spark.api.java.*;

import java.io.StringBufferInputStream;
import java.util.*;

import scala.Serializable;
import scala.Tuple2;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;


public class SparkKNN {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("JAVA KNN").master("local").getOrCreate();
		Long time = System.currentTimeMillis();
		spark.sparkContext().setLogLevel("WARN");
		JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
		if (args.length < 4) {
			System.err.println("Not enough args");
			System.exit(1);
		}
		int numNN = Integer.parseInt((args[0]));
		String trainData = args[1];
		String rawData = args[2];

		final Broadcast<Integer> broadcastK = context.broadcast(numNN);
		final JavaRDD<CoverType> trainPoints = spark.read().textFile(trainData).javaRDD().map(l -> new CoverType(l))
				.cache();

		CoverType min = getMin(trainPoints);
		CoverType max = getMax(trainPoints);
		final JavaRDD<CoverType> normalizedTrainPoints = nomalize(trainPoints, min, max);

		JavaRDD<CoverType> tests = spark.read().textFile(rawData).javaRDD().map(l -> new CoverType(l)).cache();
		tests = nomalize(tests, min, max);

		JavaPairRDD<CoverType, CoverType> paired = tests.cartesian(normalizedTrainPoints);

		JavaPairRDD<String, Tuple2<Integer, Double>> distances = paired.mapToPair(x -> {

			double distance = x._1.Differ(x._2);

			return new Tuple2<String, Tuple2<Integer, Double>>(x._1.toString(),

					new Tuple2<Integer, Double>(x._2.classifier, distance));
		});

		JavaPairRDD<String, List<Tuple2<Integer, Double>>> distancesGroupByPoints = distances
				.aggregateByKey(new ArrayList<Tuple2<Integer, Double>>(), (a, b) -> {
					a.add(b);
					a.sort(new CoverTypeComparator());
					if (a.size() <= broadcastK.value())
						return a;
					ArrayList<Tuple2<Integer, Double>> d = new ArrayList<>(a.subList(0, numNN));
					return d;
				}, (a, b) -> {
					ArrayList<Tuple2<Integer, Double>> c = new ArrayList<>(a);
					for (Tuple2<Integer, Double> t : b) {
						c.add(t);
					}
					c.sort(new CoverTypeComparator());
					if (c.size() <= broadcastK.value()) {
						return c;
					}
					ArrayList<Tuple2<Integer, Double>> d = new ArrayList<>(c.subList(0, numNN));
					return d;
				});

		JavaPairRDD<String, Integer> knnOutput = distancesGroupByPoints

				.mapValues(new Function<List<Tuple2<Integer, Double>>, Integer>() {

					@Override
					public Integer call(List<Tuple2<Integer, Double>> c) throws Exception {
						Integer k = broadcastK.value();
						// keep only k-nearest-neighbors
						// List<Tuple2<Integer, Double>> nearestK =
						// Utils.findNearestK(c, k);
						Map<Integer, Integer> majority = Utils.buildClassificationCount(c);
						Integer selectedClassification = Utils.classifyByMajority(majority);
						return selectedClassification;

					}

				});

		System.out.println("Points have been classified, writing new text file to: " + args[3]);
		long count = tests.count();

		int correct = knnOutput.map(x -> new CoverType(x._1).classifier.equals(x._2) ? 1 : 0).reduce((x, y) -> x + y);

		System.out.printf("Accuracy on test %d/%d = %.5f\r\n in: %ds", correct, count, (correct * 100.0) / count,
				(System.currentTimeMillis() - time) / 1000);

		spark.stop();
	}

	public static CoverType getMin(JavaRDD<CoverType> points) {
		return points.reduce((a, b) -> {
			double[] arr = new double[a._data.length];
			for (int i = 0; i < arr.length; ++i) {
				arr[i] = Math.min(a._data[i], b._data[i]);
			}
			return new CoverType(arr, null, null);
		});
	}

	public static JavaRDD<CoverType> nomalize(JavaRDD<CoverType> points, CoverType min, CoverType max) {
		return points.map(x -> {
			double[] arr = new double[x._data.length];
			for (int i = 0; i < arr.length; ++i) {
				arr[i] = (x._data[i] - min._data[i]) / (max._data[i] - min._data[i]);
			}
			return new CoverType(arr, x.Id, x.classifier);
		});
	}

	public static CoverType getMax(JavaRDD<CoverType> points) {
		return points.reduce((a, b) -> {
			double[] arr = new double[a._data.length];
			for (int i = 0; i < arr.length; ++i) {
				arr[i] = Math.max(a._data[i], b._data[i]);
			}
			return new CoverType(arr, null, null);
		});
	}
}
