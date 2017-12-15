package cn.edu.tsinghua.postback.receiver;

import java.io.*;
import java.math.BigInteger;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import org.apache.thrift.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceImp implements Service.Iface {
	private String uuid;
	private static final Logger LOGGER = LoggerFactory.getLogger(ServiceImp.class);

	public void getUUID(String uuid) throws TException {
		this.uuid = uuid;
	}

	public String startReceiving(String md5, String filePath, ByteBuffer dataToReceive) throws TException {
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
			byte[] buffer = new byte[1024];
			int length = -1;
			while ((length = fis.read(buffer, 0, 1024)) != -1) {
				md.update(buffer, 0, length);
			}
			md5OfReceiver = (new BigInteger(1, md.digest())).toString(16);
			fis.close();
		} catch (Exception e) {
			LOGGER.error("IoTDB post back receicer: cannot generate md5 because {}", e);
		}
		return md5OfReceiver;
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
}