import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Client {

	private static final Logger logger = LoggerFactory.getLogger(Client.class);

	private static final int MS_PER_FRAME = 16;
	private static final int HEARTBEAT_INTERVAL_MS = 1000;

	private final String clientId;
	private ZContext ctx;
	private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private ZMQ.Socket zSocket;
	private long lastHeartBeat;

	public Client(String clientId) {
		this.clientId = clientId;
		ctx = new ZContext();
		zSocket = ctx.createSocket(ZMQ.DEALER);
		zSocket.setIdentity(clientId.getBytes());
	}

	public Client connect(String endpoint) {
		if (!zSocket.connect(endpoint)) {
			throw new IllegalStateException("cannot establish connection.");
		}

		// 60 FPS main loop
		scheduler.scheduleAtFixedRate(this::mainloop, 0, MS_PER_FRAME, TimeUnit.MILLISECONDS);

		lastHeartBeat = System.currentTimeMillis();

		return this;
	}

	public void destroy() {
		if (ctx != null) {
			logger.info("[{}] Destroy context", this.clientId);
			ctx.close();
			ctx.destroy();
		}

		scheduler.shutdown();
	}

	private void mainloop() {
		long current = System.currentTimeMillis();

		heartbeat(current - lastHeartBeat);
		processMessage();
	}

	private void heartbeat(long deltaMillisFromLast) {
		if (deltaMillisFromLast < HEARTBEAT_INTERVAL_MS) {
			return;
		}

		String msg = String.format("heartbeat #%d", System.currentTimeMillis());
		if (zSocket.send(msg, 0)) {
			lastHeartBeat += deltaMillisFromLast;
			logger.info("[{}] Send msg [{}] to [{}]", this.clientId, msg, zSocket.getLastEndpoint());
		}
	}

	private void processMessage() {
		ZMsg msg = ZMsg.recvMsg(zSocket, false);
		if (msg != null) {
			//msg.getLast().print(identity);
			ZFrame recvMsg = msg.pop();
			logger.info("[{}] Recv msg [{}] from [{}]", this.clientId, recvMsg, zSocket.getLastEndpoint());
			msg.destroy();
		}
	}
}
