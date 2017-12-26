package cn.edu.tsinghua.postback.iotdb.receiver;

import java.io.*;
import java.math.BigInteger;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

public class ServiceImp implements Service.Iface {
	private static final String IOTDB_DATA_DIRECTORY = "D:" + File.separator + "iotdb-v0.3.0-server" + File.separator
			+ "data" + File.separator;
	// private static final String IOTDB_DATA_DIRECTORY = "D:\\data" +
	// File.separator;
	private String uuid;
	private Connection connection = null;
	private Statement statement = null;
	private Map<String, Set<String>> newFilesMap = new HashMap<>(); // String means Storage Group, Set means the set of
																	// new Files(AbsulutePath)
	private Map<String, Set<String>> oldFilesMap = new HashMap<>();
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceImp.class);

	public String getUUID(String uuid) throws TException {
		this.uuid = uuid;
		try {
			Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return this.uuid;
	}

	public String startReceiving(String md5, String filePath, ByteBuffer dataToReceive) throws TException {
		filePath = IOTDB_DATA_DIRECTORY + uuid + File.separator + filePath;
		File file = new File(filePath);
		if (!file.getParentFile().exists()) {
			try {
				file.getParentFile().mkdirs();
				file.createNewFile();
			} catch (IOException e) {
				LOGGER.error("IoTDB post back receicer: cannot make file because {}", e);
			}
		}
		try {
			FileOutputStream fos = new FileOutputStream(file);
			FileChannel channel = fos.getChannel();
			channel.write(dataToReceive);
			channel.close();
			fos.close();
		} catch (Exception e) {
			LOGGER.error("IoTDB post back receicer: cannot write data to file because {}", e);
		}

		String md5OfReceiver = null;
		try {
			FileInputStream fis = new FileInputStream(filePath);
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] buffer = new byte[10240];
			int length = -1;
			while ((length = fis.read(buffer, 0, 10240)) != -1) {
				md.update(buffer, 0, length);
			}
			md5OfReceiver = (new BigInteger(1, md.digest())).toString(16);
			fis.close();
		} catch (Exception e) {
			LOGGER.error("IoTDB post back receicer: cannot generate md5 because {}", e);
		}
		return md5OfReceiver;
	}

	public void getSchema(String sql) throws TException {
		try {
			if (statement == null) {
				connection = DriverManager.getConnection("jdbc:tsfile://localhost:6667/", "root", "root");
				statement = connection.createStatement();
			}
			statement.execute(sql);
		} catch (SQLException e) {
		}
	}

	/**
	 * Close connection of derby database after receiving all files from * sender
	 * side
	 */
	public void afterReceiving() throws TException {

	}

	/**
	 * Stop receiving files from sender side
	 */
	public void cancelReceiving() throws TException {

	}

	public void judgeMergeType() throws TException {
		String filePath = IOTDB_DATA_DIRECTORY + uuid + File.separator + "delta";
		File root = new File(filePath);
		File[] files = root.listFiles();
		oldFilesMap.clear();
		newFilesMap.clear();
		for (File file : files) {
			oldFilesMap.clear();
			newFilesMap.clear();
			String storageGroupPath = IOTDB_DATA_DIRECTORY + "delta" + File.separator + file.getName();
			String storageGroupPathPB = IOTDB_DATA_DIRECTORY + uuid + File.separator + "delta" + File.separator
					+ file.getName();
			String digestPath = IOTDB_DATA_DIRECTORY + "digest" + File.separator + file.getName();
			String digestPathPB = IOTDB_DATA_DIRECTORY + uuid + File.separator + "digest" + File.separator
					+ file.getName();
			File storageGroup = new File(storageGroupPath);
			File storageGroupPB = new File(storageGroupPathPB);
			File digest = new File(digestPath);
			File digestDB = new File(digestPathPB);
			if (!storageGroup.exists()) // the first type: new storage group
			{
				storageGroup.mkdirs();
				// copy the storage group
				File[] filesSG = storageGroupPB.listFiles();
				for (File fileTF : filesSG) { // file means TsFiles
					Path link = FileSystems.getDefault().getPath(storageGroupPath + File.separator + fileTF.getName());
					Path target = FileSystems.getDefault().getPath(fileTF.getAbsolutePath());
					try {
						Files.createLink(link, target);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				// copy the metadata
				digest.mkdirs();
				Path link = FileSystems.getDefault().getPath(digestPath + File.separator + file.getName() + ".restore");
				Path target = FileSystems.getDefault()
						.getPath(digestPathPB + File.separator + file.getName() + ".restore");
				try {
					Files.createLink(link, target);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else // the two other types:new tsFile but not new Storage Group , not new tsFile
			{
				HashSet<String> newFiles = new HashSet<>();
				HashSet<String> oldFiles = new HashSet<>();
				Map<String, Long> endTimeMap = new HashMap<>();
				File[] filesSG = storageGroup.listFiles();
				// get all timeseries detail endTime
				for (File fileTF : filesSG) {
					try {
						TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(
								fileTF.getAbsolutePath());
						FileReader reader = new FileReader(input);
						Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
						Iterator<String> it = deltaObjectMap.keySet().iterator();
						while (it.hasNext()) {
							String key = it.next().toString(); // key represent storage group
							TsDeltaObject deltaObj = deltaObjectMap.get(key);
							TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
							blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
									deltaObj.offset, deltaObj.metadataBlockSize));
							List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
							for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
								List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
										.getTimeSeriesChunkMetaDataList();
								for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
									TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
											.getTInTimeSeriesChunkMetaData();
									TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
									String measurementUID = properties.getMeasurementUID();
									long endTime = tInTimeSeriesChunkMetaData.getEndTime();
									measurementUID = key +  "." + measurementUID;
									if (!endTimeMap.containsKey(measurementUID))
										endTimeMap.put(measurementUID, endTime);
									else {
										if (endTimeMap.get(measurementUID) < endTime)
											endTimeMap.put(measurementUID, endTime);
									}
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				System.out.println(endTimeMap);
				// judge uuid TsFile is new file or not
				filesSG = storageGroupPB.listFiles();
				for (File fileTF : filesSG) {
					boolean isNew = true;
					try {
						TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(
								fileTF.getAbsolutePath());
						FileReader reader = new FileReader(input);
						Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
						Iterator<String> it = deltaObjectMap.keySet().iterator();
						while (it.hasNext()) {
							String key = it.next().toString(); // key represent storage group
							TsDeltaObject deltaObj = deltaObjectMap.get(key);
							TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
							blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
									deltaObj.offset, deltaObj.metadataBlockSize));
							List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
							for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
								List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
										.getTimeSeriesChunkMetaDataList();
								for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
									TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
											.getTInTimeSeriesChunkMetaData();
									TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
									String measurementUID = properties.getMeasurementUID();
									measurementUID = key +  "." + measurementUID;
									long startTime = tInTimeSeriesChunkMetaData.getStartTime();
									if (endTimeMap.containsKey(measurementUID) && endTimeMap.get(measurementUID) >= startTime)
									{
										isNew = false;
										break;
									}
								}
								if(!isNew)
									break;
							}
							if(!isNew)
								break;
						}
						if(isNew)
							newFiles.add(fileTF.getAbsolutePath());
						else
							oldFiles.add(fileTF.getAbsolutePath());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				if (newFiles.size() != 0)
					newFilesMap.put(file.getName(), newFiles);
				if (oldFiles.size() != 0)
					oldFilesMap.put(file.getName(), oldFiles);
			}
		}
		System.out.println("最终结果new：");
		System.out.println(newFilesMap);
		System.out.println("最终结果old：");
		System.out.println(oldFilesMap);
	}

	public void mergeBySQL() throws TException {

	}

	public void mergeNewData() throws TException {

	}

	public void test() {
		uuid = "uuid";
		try {
			judgeMergeType();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ServiceImp service = new ServiceImp();
		service.test();
	}
}