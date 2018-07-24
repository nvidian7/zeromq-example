import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.*;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

class Server {

	private static final Logger logger = LoggerFactory.getLogger(Server.class);
	private static final String BACKEND_ENDPOINT = "inproc://backend";

	private ZContext ctx;
	private int workerCount;
	private Set<Thread> workers = Sets.newLinkedHashSet();

	public Server(int workerCount) {
		this.workerCount = workerCount;
	}

	public Server bind(String address) {
		ctx = new ZContext();

		ZMQ.Socket frontend = ctx.createSocket(ZMQ.ROUTER);
		frontend.bind(address);

		ZMQ.Socket backend = ctx.createSocket(ZMQ.DEALER);
		backend.bind(BACKEND_ENDPOINT);

		ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("server-bootstrapper").build();
		Executors.newSingleThreadExecutor(tf).submit(() -> {
			logger.info("Connect backend to frontend via a proxy");
			ZMQ.proxy(frontend, backend, null);
		});

		for (int i = 0; i < workerCount; i++) {
			Thread worker = new Thread(new Worker(ctx));
			worker.setName(String.format("server-worker-%d", i+1));
			worker.start();
			workers.add(worker);
		}

		return this;
	}

	public void destroy() {
		if (ctx != null) {
			logger.info("[Server] Destroy zeromq context");
			ctx.destroy();
		}
	}

	public static class Worker implements Runnable {

		private static final Logger logger = LoggerFactory.getLogger(Worker.class);

		private ZContext ctx;
		private ZMQ.Socket socket;

		public Worker(ZContext ctx) {
			this.ctx = ctx;
		}

		public void run() {
			socket = ctx.createSocket(ZMQ.DEALER);
			socket.connect(BACKEND_ENDPOINT);

			logger.info("Starting worker thread");

			while (!Thread.currentThread().isInterrupted()) {
				try {
					//  The DEALER socket gives us the address envelope and message
					ZMsg msg = ZMsg.recvMsg(socket); // blocked here
					ZFrame address = msg.pop();
					ZFrame content = msg.pop();
					logger.info("Recv msg [{}] from address[{}]", content, address);
					assert (content != null);
					msg.destroy();

					address.send(socket, ZFrame.REUSE + ZFrame.MORE);
					content.send(socket, ZFrame.REUSE);

					address.destroy();
					content.destroy();
				} catch(ZMQException e) {
					if(e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
						logger.warn("Connection closed");
						break;
					}
				}
			}

			logger.info("Worker finished.");
		}
	}
}
