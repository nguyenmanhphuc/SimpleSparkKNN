package knearestneighbor.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

public class RandomTest {

	public static void main(String[] args) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\nguye\\Desktop\\covtype.data"));
		PrintWriter data = new PrintWriter("C:\\Users\\nguye\\Desktop\\data.txt");
		PrintWriter test = new PrintWriter("C:\\Users\\nguye\\Desktop\\test.txt");
		Random rand = new Random();
		for (int i = 0; true; ++i) {
			String str = reader.readLine();
			if (str == null || str.length() == 0) {
				break;
			}
			if (rand.nextInt(100) < 2) {
				test.println(i + "," + str);
			} else {
				data.println(i + "," + str);

			}
		}
		data.close();
		test.close();
	}

}
