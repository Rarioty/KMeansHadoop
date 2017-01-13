package main.java.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {
	public int nbDimensions;
	public Double[] dimensions;
	public int weight;
	
	public PointWritable() {
		nbDimensions = 1;
		dimensions = new Double[1];
	}
	
	public PointWritable(int column, Double init[], int weight) {
		nbDimensions = column;
		dimensions = new Double[column];
		
		for (int i = 0; i < init.length && i < column; ++i)
		{
			dimensions[i] = init[i];
		}
		
		this.weight = weight;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nbDimensions = in.readInt();
		dimensions = new Double[nbDimensions];
		for (int i = 0; i < nbDimensions; ++i)
		{
			dimensions[i] = in.readDouble();
		}
		weight = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nbDimensions);
		for (int i = 0; i < nbDimensions; ++i)
		{
			out.writeDouble(dimensions[i]);
		}
		out.writeInt(weight);
	}

	@Override
	public String toString() {
		String result = "";
		
		for (int i = 0; i < nbDimensions-1; ++i)
		{
			result += dimensions[i] + ",";
		}
		result += dimensions[nbDimensions-1];
		
		result += "\t" + weight;
		
		return result;
	}
}
