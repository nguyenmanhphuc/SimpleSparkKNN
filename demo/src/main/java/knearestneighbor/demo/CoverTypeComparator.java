package knearestneighbor.demo;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple2;

public class CoverTypeComparator implements Serializable, Comparator<Tuple2<Integer, Double>> {

	@Override
	public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
		return Double.compare(o1._2, o2._2);
	}

}
