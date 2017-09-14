package knearestneighbor.demo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class SerialKNN {
	public static void main(String[] args) throws IOException {
		Long start = System.currentTimeMillis();
		int k = Integer.parseInt(args[0]);
		String trainDataFile = args[1];
		String testDataFile = args[2];
		String resFile = args[3];
		CoverType[] trainData = readTrainData(trainDataFile);
		BufferedReader reader = new BufferedReader(new FileReader(testDataFile));

		// Begin to read and process test data
		String s = null;
		int count = 0;
		int correct = 0;
		while ((s = reader.readLine()) != null) {
			count++;
			if (count % 1000 == 1) {
				System.out.println(count);
			}
			CoverType test = new CoverType(s);
			List<Tuple2<Integer, Double>> distances = getDistances(test, trainData);
			List<Tuple2<Integer, Double>> kNearestNeighbors = Utils.findNearestK(new ArrayList<>(distances), k);
			Map<Integer, Integer> c = Utils.buildClassificationCount(kNearestNeighbors);
			Integer selectedClassification = Utils.classifyByMajority(c);
			if (test.classifier == selectedClassification) {
				correct++;
			}
		}
		System.out.printf("Total execute time: %ds", (System.currentTimeMillis() - start) / 1000);
		System.out.printf("Accuracy on test %d/%d = %.5f\r\n", correct, count, (correct * 100.0) / count);

	}

	private static List<Tuple2<Integer, Double>> getDistances(CoverType test, CoverType[] trainData) {
		List<Tuple2<Integer, Double>> distances = new ArrayList<Tuple2<Integer, Double>>();
		for (int i = 0; i < trainData.length; ++i) {
			distances.add(new Tuple2<Integer, Double>(trainData[i].classifier, trainData[i].Differ(test)));
		}
		return distances;
	}

	private static CoverType[] readTrainData(String trainDataFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(trainDataFile));
		String s = null;
		List<CoverType> data = new ArrayList<>();
		while ((s = reader.readLine()) != null) {
			data.add(new CoverType(s));
		}
		CoverType[] arr = new CoverType[data.size()];
		return data.toArray(arr);
	}
}