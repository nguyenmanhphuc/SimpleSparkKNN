package knearestneighbor.demo;

import scala.Serializable;

@SuppressWarnings("serial")
public class CoverType implements Serializable {
	double[] _data;
	public Integer Id;
	public Integer classifier;

	public double[] getData() {
		return _data;
	}

	public CoverType(String templatedString) {
		String[] data = templatedString.split(",");
		Id = Integer.parseInt(data[0]);
		if (Id == null) {
			System.out.println("");
		} else {

		}
		_data = new double[data.length - 2];
		for (int i = 1; i < data.length - 1; ++i) {
			_data[i - 1] = Double.parseDouble(data[i]);
		}
		classifier = Integer.parseInt(data[data.length - 1]);

	}

	@Override
	public boolean equals(Object obj) {
		return this.Id == ((CoverType) obj).Id;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Id;
	}

	public CoverType(double[] data, Integer id, Integer classifier) {
		this._data = data;
		this.Id = id;
		this.classifier = classifier;
	}

	public double getClassifier() {
		return classifier;
	}

	public double Differ(CoverType type) {

		double value = 0;
		for (int i = 0; i < _data.length; ++i) {
			value += Math.pow((this._data[i] - type.getData()[i]), 2);
		}
		return Math.sqrt(value);
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(Id);
		for (int i = 0; i < _data.length; ++i) {
			str.append(",");
			str.append(_data[i]);
		}
		str.append(",");
		str.append(classifier);
		return str.toString();
	}
}
