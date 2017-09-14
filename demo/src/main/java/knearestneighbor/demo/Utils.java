package knearestneighbor.demo;

import java.util.ArrayList;

import java.util.Comparator;

import java.util.HashMap;

import java.util.List;

import java.util.Map;

import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

public class Utils {

	public static <T> List<Tuple2<T, Double>> findNearestK(Iterable<Tuple2<T, Double>> neighbors, int k) {

		List<Tuple2<T, Double>> list = new ArrayList<>();
		neighbors.iterator().forEachRemaining(list::add);
		list.sort(new Comparator<Tuple2<T, Double>>() {
			@Override
			public int compare(Tuple2<T, Double> t1, Tuple2<T, Double> t2) {
				return t1._2.compareTo(t2._2);
			}
		});

		if (list.size() < k) {
			return list;
		}

		return list.subList(0, k);
	}

	public static <T> Map<T, Integer> buildClassificationCount(List<Tuple2<T, Double>> nearestK) {

		Map<T, Integer> res = new HashMap<T, Integer>();

		nearestK.forEach(x -> {
			Integer count = res.get(x._1);
			count = count == null ? 1 : (count + 1);
			res.put(x._1, count);
		});

		return res;

	}

	public static <T> T classifyByMajority(Map<T, Integer> majority) {
		T ans = null;
		int max = -1;
		for (Entry<T, Integer> s : majority.entrySet()) {
			if (max < s.getValue()) {
				max = s.getValue();
				ans = s.getKey();
			}
		}
		return ans;

	}

	public static List<Tuple2<Integer, Double>> findNearestK(JavaPairRDD<Integer, Double> distances, Integer k) {

		List<Tuple2<Integer, Double>> sorted = distances.takeOrdered(k, new CoverTypeComparator());

		return sorted;

	}

	public static Double buildClassificationCount1(List<Tuple2<Integer, Double>> nearestK) {

		Map<Integer, Integer> res = new HashMap<Integer, Integer>();

		int max = -1;

		double ans = -1;

		for (Tuple2<Integer, Double> x : nearestK) {

			Integer count = res.get(x._1);

			count = count == null ? 1 : (count + 1);

			res.put(x._1, count);

			if (max < count) {
				max = count;
				ans = x._1;
			}

		}

		return ans;

	}
}