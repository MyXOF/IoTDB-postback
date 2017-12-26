package cn.edu.tsinghua.postback.iotdb.receiver;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.Action;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.FileNodeConstants;
import cn.edu.tsinghua.iotdb.engine.filenode.*;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
public class RestoreTest {
	String nameSpacePath = "root.vehicle";
	public String fileNodeDirPath = "D:\\iotdb-v0.3.0\\data\\digest" + File.separatorChar;
	public static void main(String[] args) throws Exception {
		RestoreTest test = new RestoreTest();
		test.read();
	}
	public void read() throws Exception {
		
//		System.out.println(fileNodeDirPath);
//		if (fileNodeDirPath.length() > 0
//				&& fileNodeDirPath.charAt(fileNodeDirPath.length() - 1) != File.separatorChar) {
//			fileNodeDirPath = fileNodeDirPath + File.separatorChar;
//		}
//		System.out.println(fileNodeDirPath);
//		String dataDirPath = fileNodeDirPath + nameSpacePath;
//		File dataDir = new File(dataDirPath);
//		if (!dataDir.exists()) {
//			dataDir.mkdirs();
//			System.out.println("The filenode processor data dir doesn't exists, and mkdir the dir");
//		}
//		String fileNodeRestoreFilePath = new File(dataDir, nameSpacePath + ".restore").getAbsolutePath();
//		System.out.println(fileNodeRestoreFilePath);
		FileNodeProcessor processor = new FileNodeProcessor(fileNodeDirPath, nameSpacePath, null);
		Map<String, Long> lastUpdateTimeMap = processor.getLastUpdateTimeMap();
		Iterator iter = lastUpdateTimeMap.keySet().iterator();
		while (iter.hasNext()) {
			String a = iter.next().toString();
			System.out.println( "subject" + a);
			System.out.println(lastUpdateTimeMap.get(a));
		}
		System.out.println(lastUpdateTimeMap);
	}

}
