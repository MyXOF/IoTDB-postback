package cn.edu.tsinghua.postback.sender;

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
import java.security.MessageDigest;
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

import cn.edu.tsinghua.postback.receiver.Service;
import cn.edu.tsinghua.postback.Config;

public class TransferData {

	private TTransport transport;
	private Service.Client clientOfServer;
	private String uuid;
	private static final Logger LOGGER = LoggerFactory.getLogger(TransferData.class);

	private static class InitBeforeSendingHolder {
		private static final TransferData INSTANCE = new TransferData();
	}

	private TransferData() {
	}

	public static final TransferData getInstance() {
		return InitBeforeSendingHolder.INSTANCE;
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

	public void transferUUID(String uuidPath) {
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
			clientOfServer.getUUID(uuid);
		} catch (TException e) {
			LOGGER.error("IoTDB post back sender: cannot send UUID to receiver because {}", e);
		}

	}

	public void fileSnapshot(Set<String> sendingFileList, String snapshotPath) {
		try {
			for (String filePath : sendingFileList) {
				String newPath = snapshotPath + File.separator + filePath;
				File newfile = new File(newPath);
				if (!newfile.getParentFile().exists()) { 
					newfile.getParentFile().mkdirs();
				}
				String os = System.getProperty("os.name");
				if (os.toLowerCase().startsWith("windows")) {
					String cmdCommandWin = "cmd /c mklink /H %s %s";
					String cmdCommand = String.format(cmdCommandWin, newPath, filePath);
					Runtime.getRuntime().exec(cmdCommand);
				} else {
					String commandLinux = "ln source %s %s";
					String[] command = new String[] { "/bin/sh", "-c", String.format(commandLinux, newPath, filePath) };
					Runtime.getRuntime().exec(command);
				}
			}
		} catch (IOException e) {
			LOGGER.error("IoTDB post back sender: cannot make fileSnapshot because {}", e);
		}
	}

	void startSending(Set<String> fileList, String snapshotPath) {
		try {
			for (String filePath : fileList) {
				String dataPath = snapshotPath + File.separator + filePath;
				File file = new File(dataPath);
				byte[] bytes = toByteArray(dataPath);
				ByteBuffer buffToSend = ByteBuffer.wrap(bytes);

				FileInputStream fis = new FileInputStream(file);
				MessageDigest md = MessageDigest.getInstance("MD5");
				byte[] buffer = new byte[1024];
				int length = -1;
				while ((length = fis.read(buffer, 0, 1024)) != -1) {
					md.update(buffer, 0, length);
				}
				String md5OfSender = (new BigInteger(1, md.digest())).toString(16);

				while (true) {
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

	public void sendSchema() {
		// TODO Auto-generated method stub

	}

	public void stop() {
		// TODO Auto-generated method stub

	}

	public void afterSending() {
		// TODO Auto-generated method stub

	}
}