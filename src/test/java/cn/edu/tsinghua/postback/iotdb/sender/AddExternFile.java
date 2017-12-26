package cn.edu.tsinghua.postback.iotdb.sender;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessorStatus;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessorStore;
import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.filenode.OverflowChangeType;
import cn.edu.tsinghua.iotdb.engine.filenode.SerializeUtil;
import cn.edu.tsinghua.iotdb.exception.FileNodeProcessorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

public class AddExternFile {

	public static void main(String[] args) throws Exception {
		AddExternFile addExternFile = new AddExternFile();
		addExternFile.test();
	}
	
	void test() throws FileNodeProcessorException {
		/*
		FileNodeProcessor processor = new FileNodeProcessor("D:\\iotdb-v0.3.0-server\\data\\digest", "root.testWBWJ", null);
		FileNodeProcessorStore fileNodeProcessorStore = processor.getFileNodeProcessorStore();
		Map<String, Long> lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		List<IntervalFileNode> newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		System.out.println(lastUpdateTimeMap);
		for(IntervalFileNode intervalFileNode:newFileNodes)
		{
			System.out.println(intervalFileNode.filePath);
			intervalFileNode.filePath = "D:\\iotdb-v0.3.0-server\\data\\delta\\root.testWBWJ\\1-1514186925136";
			newFileNodes.clear();
			newFileNodes.add(intervalFileNode);
		}
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		processor.writeStoreToDisk(processor.getFileNodeProcessorStore());
		*/
		String fileNodeRestoreFilePath = "D:\\iotdb-v0.3.0-server\\data\\digest\\root.testWBWJ\\root.testWBWJ.restore";
		FileNodeProcessorStore fileNodeProcessorStore = null;
		synchronized (fileNodeRestoreFilePath) {
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				fileNodeProcessorStore = serializeUtil.deserialize(fileNodeRestoreFilePath)
						.orElse(new FileNodeProcessorStore(new HashMap<>(),
								new IntervalFileNode(OverflowChangeType.NO_CHANGE, null),
								new ArrayList<IntervalFileNode>(), FileNodeProcessorStatus.NONE, 0));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
		Map<String, Long> lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		List<IntervalFileNode> newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		System.out.println(lastUpdateTimeMap);
		System.out.println("NumOfMergeFile" + fileNodeProcessorStore.getNumOfMergeFile());
		System.out.println("EmptyIntervalFileNode" + fileNodeProcessorStore.getEmptyIntervalFileNode());
		System.out.println("FileNodeProcessorState" + fileNodeProcessorStore.getFileNodeProcessorState());
//		IntervalFileNode fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,file0.getAbsolutePath());
		/*
		for(IntervalFileNode intervalFileNode:newFileNodes)
		{
			System.out.println(intervalFileNode.filePath);
			System.out.println(intervalFileNode.getStartTimeMap());
			System.out.println(intervalFileNode.getEndTimeMap());
			intervalFileNode.filePath = "D:\\iotdb-v0.3.0-server\\data\\delta\\root.testWBWJ\\1-1514186925136";
			newFileNodes.clear();
			newFileNodes.add(intervalFileNode);
		}
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		synchronized (fileNodeRestoreFilePath) {
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				serializeUtil.serialize(fileNodeProcessorStore, fileNodeRestoreFilePath);
			} catch (IOException e) {
				throw new FileNodeProcessorException(e);
			}
		}*/
	}
	
	void test1() throws IOException, Exception {
		/*
		FileNodeProcessor processor = new FileNodeProcessor("D:\\iotdb-v0.3.0-server\\data\\digest", "root.testWBWJ", null);
		FileNodeProcessorStore fileNodeProcessorStore = processor.getFileNodeProcessorStore();
		Map<String, Long> lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		List<IntervalFileNode> newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		System.out.println(lastUpdateTimeMap);
		for(IntervalFileNode intervalFileNode:newFileNodes)
		{
			System.out.println(intervalFileNode.filePath);
			intervalFileNode.filePath = "D:\\iotdb-v0.3.0-server\\data\\delta\\root.testWBWJ\\1-1514186925136";
			newFileNodes.clear();
			newFileNodes.add(intervalFileNode);
		}
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		processor.writeStoreToDisk(processor.getFileNodeProcessorStore());
		*/
		String fileNodeRestoreFilePath = "D:\\iotdb-v0.3.0-server\\data\\digest\\root.testWBWJ\\root.testWBWJ.restore";
		FileNodeProcessorStore fileNodeProcessorStore = null;
		synchronized (fileNodeRestoreFilePath) {
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				fileNodeProcessorStore = serializeUtil.deserialize(fileNodeRestoreFilePath)
						.orElse(new FileNodeProcessorStore(new HashMap<>(),
								new IntervalFileNode(OverflowChangeType.NO_CHANGE, null),
								new ArrayList<IntervalFileNode>(), FileNodeProcessorStatus.NONE, 0));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileNodeProcessorException(e);
			}
		}
		Map<String, Long> lastUpdateTimeMap = fileNodeProcessorStore.getLastUpdateTimeMap();
		List<IntervalFileNode> newFileNodes = fileNodeProcessorStore.getNewFileNodes();
		System.out.println("lastUpdateTimeMap:");
		System.out.println(lastUpdateTimeMap);
		System.out.println();
		/**
		 * 打开TsFile文件进行解析
		 */
		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();
		String path = "D:\\iotdb-v0.3.0-server\\data\\delta\\root.testWBWJ\\30-1514258861189";
        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(path);           
        FileReader reader = new FileReader(input);            
        Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
        Iterator<String> it = deltaObjectMap.keySet().iterator();
        while(it.hasNext()) {
        	String key = it.next().toString(); //key represent storage group
        	TsDeltaObject deltaObj = deltaObjectMap.get(key);
        	TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
        	System.out.println("blockMeta ID:" + blockMeta.getDeltaObjectID());
            blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input, deltaObj.offset, deltaObj.metadataBlockSize));
            List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
            for(RowGroupMetaData rowGroupMetaData:rowGroupMetadataList)
            {
            	long startTime = 0x7fffffff;
            	long endTime = 0;
            	System.out.println("rowGroupMetaData ID:" + rowGroupMetaData.getDeltaObjectID());
            	List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
            	for(TimeSeriesChunkMetaData timeSeriesChunkMetaData:timeSeriesChunkMetaDataList)
            	{
            		TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData();
            		TimeSeriesChunkProperties properties =timeSeriesChunkMetaData.getProperties();
            		String measurementUID = properties.getMeasurementUID();
            		startTime = Math.min(tInTimeSeriesChunkMetaData.getStartTime(),startTime);
            		endTime = Math.max(tInTimeSeriesChunkMetaData.getEndTime(), endTime);
            	}
            	startTimeMap.put(rowGroupMetaData.getDeltaObjectID(), startTime);
            	endTimeMap.put(rowGroupMetaData.getDeltaObjectID(), endTime);
            }
        }
		
		
		
		//*******************************
		IntervalFileNode fileNode = new IntervalFileNode(startTimeMap, endTimeMap, OverflowChangeType.NO_CHANGE,path);
		for(IntervalFileNode intervalFileNode:newFileNodes)
		{
			System.out.println(intervalFileNode.filePath);
			System.out.println(intervalFileNode.getStartTimeMap());
			System.out.println(intervalFileNode.getEndTimeMap());
		}
		System.out.println("fileNode:");
		System.out.println(fileNode.filePath);
		System.out.println(fileNode.getStartTimeMap());
		System.out.println(fileNode.getEndTimeMap());
		// modify fileNodeProcessorStore.newFileNodes
		newFileNodes.add(fileNode);
		fileNodeProcessorStore.setNewFileNodes(newFileNodes);
		// modify fileNodeProcessorStore.lastUpdateTimeMap
		for(String deltaObject:lastUpdateTimeMap.keySet())
		{
			if(endTimeMap.containsKey(deltaObject))
			{
				long lastUpdataTime = lastUpdateTimeMap.get(deltaObject);
				if(lastUpdataTime < endTimeMap.get(deltaObject))
				{
					lastUpdateTimeMap.put(deltaObject, endTimeMap.get(deltaObject));
				}
			}
		}
		fileNodeProcessorStore.setLastUpdateTimeMap(lastUpdateTimeMap);
		synchronized (fileNodeRestoreFilePath) {
			SerializeUtil<FileNodeProcessorStore> serializeUtil = new SerializeUtil<>();
			try {
				serializeUtil.serialize(fileNodeProcessorStore, fileNodeRestoreFilePath);
			} catch (IOException e) {
				throw new FileNodeProcessorException(e);
			}
		}
	}
}
