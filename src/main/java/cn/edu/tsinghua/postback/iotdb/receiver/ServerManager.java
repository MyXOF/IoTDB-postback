package cn.edu.tsinghua.postback.iotdb.receiver;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerManager {
	private TServerSocket serverTransport;
	private Factory protocolFactory;
	private TProcessor processor;
	private TThreadPoolServer.Args poolArgs;
	private TServer poolServer;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);
	private static class ServerManagerHolder{
		private static final ServerManager INSTANCE = new ServerManager();
	}
	
	private ServerManager() {}

	public static final ServerManager getInstance() {
		return ServerManagerHolder.INSTANCE;
	}

	public void startServer(int serverPort) {
		try {
			serverTransport = new TServerSocket(serverPort);
			protocolFactory = new TBinaryProtocol.Factory();
			processor = new Service.Processor<ServiceImp>(new ServiceImp());
			poolArgs = new TThreadPoolServer.Args(serverTransport);
			poolArgs.processor(processor);
			poolArgs.protocolFactory(protocolFactory);
			poolServer = new TThreadPoolServer(poolArgs);
			System.out.println("Server start!");
			Runnable runnable = new Runnable() {
				public void run() {
					poolServer.serve();
				}
			};
			new Thread(runnable).start();
		} catch (TTransportException e) {
			LOGGER.error("IoTDB post back receicer: cannot start server because {}", e);
		}
	}

	public void closeServer() {
		poolServer.stop();
	}
}
