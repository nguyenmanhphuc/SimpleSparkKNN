package knearestneighbor.demo;

import org.apache.spark.api.java.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import scala.Tuple2;

import org.apache.spark.broadcast.Broadcast;

import org.apache.spark.sql.SparkSession;

public class ParallelizedKNN {

	public static void main(String[] args) throws IOException {

		SparkSession spark = SparkSession.builder().master("local").appName("JAVA KNN").getOrCreate();

		spark.sparkContext().setLogLevel("WARN");
		long time = System.currentTimeMillis();

		JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());

		if (args.length < 4) {

			System.err.println("Not enough args");

			System.exit(1);

		}

		int numNN = Integer.parseInt((args[0]));

		String trainData = args[1];

		String rawData = args[2];

		final Broadcast<Integer> broadcastK = context.broadcast(numNN);

		JavaRDD<CoverType> trainPoints = spark.read().textFile(trainData).javaRDD().map(l -> new CoverType(l))

				.cache();
		BufferedReader reader = new BufferedReader(new FileReader(rawData));
		final int[] summary = new int[2];
		// CoverType min = getMin(trainPoints);
		// //
		// CoverType max = getMax(trainPoints);
		// //
		// trainPoints = nomalize(trainPoints, min, max).cache();

		// JavaRDD<CoverType> tests =
		// spark.read().textFile(rawData).javaRDD().map(l -> new
		// CoverType(l)).cache();

		// tests = nomalize(tests, min, max);
		int count = 0;
		int correct = 0;
		String s = null;
		while ((s = reader.readLine()) != null) {
			// List<CoverType> types = tests.collect();
			// CoverType t = normalize( new CoverType(s), min, max);
			CoverType t = new CoverType(s);
			JavaPairRDD<Integer, Double> distances = trainPoints.mapToPair(x -> {

				double d = x.Differ(t);

				return new Tuple2<Integer, Double>(x.classifier, d);

			});

			List<Tuple2<Integer, Double>> nearestK = Utils.findNearestK(distances, broadcastK.value());

			Double ans = Utils.buildClassificationCount1(nearestK);

			if (ans == t.getClassifier()) {
				correct++;
			}
			count++;

		}
		System.out.printf("Accuracy on test %d/%d\r\n in: %d", correct, count,
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

	public static CoverType normalize(CoverType point, CoverType min, CoverType max) {
		double[] arr = new double[point._data.length];
		for (int i = 0; i < arr.length; ++i) {
			arr[i] = (point._data[i] - min._data[i]) / (max._data[i] - min._data[i]);
		}
		return new CoverType(arr, point.Id, point.classifier);
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
