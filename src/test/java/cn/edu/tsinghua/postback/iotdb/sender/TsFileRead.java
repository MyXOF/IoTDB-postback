package cn.edu.tsinghua.postback.iotdb.sender;
/**
 * The class is to show how to read TsFile file named "test.ts".
 * The TsFile file "test.ts" is generated from class TsFileWrite1 or class TsFileWrite2, 
 * they generate the same TsFile file by two different ways
 */

import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileRead {

	public static void main(String[] args) throws IOException {

		String path = "D:\\iotdb-v0.3.0\\data\\delta\\root.test\\1-1514185554714";

		// read example : no filter
		TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(path);
		TsFile readTsFile = new TsFile(input);
		ArrayList<Path> paths = new ArrayList<>();
		paths.add(new Path("root.test.d0.s0"));
		paths.add(new Path("root.test.d0.s1"));
		QueryDataSet queryDataSet = readTsFile.query(paths, null, null);
		while (queryDataSet.hasNextRecord()) {
			List<Field> fields = queryDataSet.getNextRecord().getFields();
			System.out.println(fields);
			System.out.println();
			if(fields.get(0).toString()!="null")
				System.out.println(fields.get(0).deltaObjectId + fields.get(0).measurementId + " " + fields.get(0).toString());
			System.out.println();
			if(fields.get(1).toString()!="null")
				System.out.println(fields.get(1).deltaObjectId + fields.get(1).measurementId + " " + fields.get(1).toString());
		}
	}

}