package cn.edu.tsinghua.postback.iotdb.sender;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.client.AbstractClient;
import cn.edu.tsinghua.iotdb.jdbc.TsfileConnection;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import cn.edu.tsinghua.iotdb.postback.Config;
import cn.edu.tsinghua.postback.iotdb.receiver.ServerManager;
import cn.edu.tsinghua.postback.iotdb.receiver.Service;

import org.json.JSONObject;

public class TransferData {

	private TTransport transport;
	private Service.Client clientOfServer;
	private Set<String> schema = new HashSet<>();
	private String uuid;

	private static final Logger LOGGER = LoggerFactory.getLogger(TransferData.class);

	private static class TransferHolder {
		private static final TransferData INSTANCE = new TransferData();
	}

	private TransferData() {
	}

	public static final TransferData getInstance() {
		return TransferHolder.INSTANCE;
	}

	public void connection(String serverIP, int serverPort) {
		transport = new TSocket(serverIP, serverPort);
		TProtocol protocol = new TBinaryProtocol(transport);
		clientOfServer = new Service.Client(protocol);
		try {
			transport.open();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back sender: cannot connect to server because {}", e);
		}
	}

	public String transferUUID(String uuidPath) {
		String uuidOfReceiver = null;
		File file = new File(uuidPath);
		BufferedReader bf;
		FileOutputStream out;
		if (!file.exists()) {
			try {
				file.createNewFile();
				uuid = "PB" + UUID.randomUUID().toString().replaceAll("-", "");
				out = new FileOutputStream(file);
				out.write(uuid.getBytes());
				out.close();
			} catch (Exception e) {
				LOGGER.error("IoTDB post back sender: cannot write UUID to file because {}", e);
			}
		} else {
			try {
				bf = new BufferedReader(new FileReader(uuidPath));
				uuid = bf.readLine();
				bf.close();
			} catch (IOException e) {
				LOGGER.error("IoTDB post back sender: cannot read UUID from file because {}", e);
			}
		}
		try {
			uuidOfReceiver = clientOfServer.getUUID(uuid);
		} catch (TException e) {
			LOGGER.error("IoTDB post back sender: cannot send UUID to receiver because {}", e);
		}
		return uuidOfReceiver;
	}

	public void makeFileSnapshot(Set<String> sendingFileList, String snapshotPath, String iotdbPath) throws InterruptedException {
		try {
			for (String filePath : sendingFileList) {
				String relativeFilePath = filePath.substring(iotdbPath.length());
				String newPath = snapshotPath + File.separator + relativeFilePath;
				File newfile = new File(newPath);
				if (!newfile.getParentFile().exists()) { 
					newfile.getParentFile().mkdirs();
				}
				Path link = FileSystems.getDefault().getPath(newPath);   
				Path target = FileSystems.getDefault().getPath(filePath);   
				Files.createLink(link, target);
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot make fileSnapshot because {}", e);
		}
	}

	public void startSending(Set<String> fileList, String snapshotPath, String iotdbPath) {
		try {
			for (String filePath : fileList) {
				String relativeFilePath = filePath.substring(iotdbPath.length());
				String dataPath = snapshotPath + File.separator + relativeFilePath;
				File file = new File(dataPath);
				byte[] bytes = toByteArray(dataPath);
				ByteBuffer buffToSend = ByteBuffer.wrap(bytes);

				FileInputStream fis = new FileInputStream(file);
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] buffer = new byte[10240];
				int length = -1;
				while ((length = fis.read(buffer, 0, 10240)) != -1) {
					md.update(buffer, 0, length);
				}
				String md5OfSender = (new BigInteger(1, md.digest())).toString(16);

				while (true) {
					filePath = filePath.substring(iotdbPath.length());
					String md5OfReceiver = clientOfServer.startReceiving(md5OfSender, filePath, buffToSend);
					if (md5OfSender.equals(md5OfReceiver)) {
						break;
					}
				}
				fis.close();
			}
		} catch (Exception e) {
			LOGGER.error("IoTDB post back sender: cannot sending data because {}", e);
		}
	}

	public static byte[] toByteArray(String filePath) {
		byte[] buffer = null;
		try {
			File file = new File(filePath);
			FileInputStream fis = new FileInputStream(file);
			ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
			byte[] b = new byte[1000];
			int n;
			while ((n = fis.read(b)) != -1) {
				bos.write(b, 0, n);
			}
			fis.close();
			bos.close();
			buffer = bos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return buffer;
	}

	public void getSchema(String schemaPath) throws ClassNotFoundException, SQLException, IOException, InterruptedException{
		BufferedReader bf = new BufferedReader(new FileReader(schemaPath));
		String data;
		schema.clear();
		while((data = bf.readLine())!=null)
		{
			String item[] = data.split(",");
			if(item[0].equals("2"))
			{
				String sql = "SET STORAGE GROUP TO " + item[1];
				schema.add(sql);
			}
			else if(item[0].equals("0"))
			{
				String sql = "CREATE TIMESERIES " + item[1] + " WITH DATATYPE=" + item[2] + ", ENCODING=" + item[3];
				schema.add(sql);
			}
		}
		bf.close();
	}
	
	public void sendSchema() throws ClassNotFoundException, SQLException, IOException, InterruptedException{
		for(String sql:schema)
		{
			try {
				clientOfServer.getSchema(sql);
			} catch (TException e) {}
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, InterruptedException {
		TransferData transferData = TransferData.getInstance();
		ServerManager serverManager = ServerManager.getInstance();
		serverManager.startServer(5555);
		transferData.connection("127.0.0.1", 5555);
		String path = "D:\\iotdb-v0.3.0\\data\\metadata\\mlog.txt";
		transferData.getSchema(path);
		transferData.sendSchema();
		serverManager.closeServer();
	}

	public void stop() {
		// TODO Auto-generated method stub

	}

	public void afterSending(String snapshotPath) {
		deleteSnapshot(new File(snapshotPath));

	}
	
	public void deleteSnapshot(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} 
		else{
			File[] files = file.listFiles();
			for (File f : files) {
				deleteSnapshot(f);        
				f.delete();       
			}
		}		
	}
	
	public Set<String> getSchema() {
		return schema;
	}
	
	public String getUuid() {
		return uuid;
	}
}