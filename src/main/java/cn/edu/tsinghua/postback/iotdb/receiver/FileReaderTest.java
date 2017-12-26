package cn.edu.tsinghua.postback.iotdb.receiver;

import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;

import java.util.*;

import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;

public class FileReaderTest {
	
            public void start() throws IOException {
	            String path = "D:\\iotdb-v0.3.0-server\\data\\delta\\root.vehicle\\1508844780000-1513756157770";
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
	                	System.out.println("rowGroupMetaData ID:" + rowGroupMetaData.getDeltaObjectID());
	                	List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
	                	for(TimeSeriesChunkMetaData timeSeriesChunkMetaData:timeSeriesChunkMetaDataList)
	                	{
	                		TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData();
	                		TimeSeriesChunkProperties properties =timeSeriesChunkMetaData.getProperties();
	                		String measurementUID = properties.getMeasurementUID();
	                		long startTime = tInTimeSeriesChunkMetaData.getStartTime();
	                		long endTime = tInTimeSeriesChunkMetaData.getEndTime();
	                		System.out.println(measurementUID);
	                		System.out.println(key +  "." + measurementUID);
	                		System.out.println(startTime);
	                		System.out.println(endTime);
	                	}
	                }
	            }
            }
			public static void main(String[] args) throws IOException, WriteProcessException{
				FileReaderTest test = new FileReaderTest();
				test.start();
				}

}
