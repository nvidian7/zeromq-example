public class Application {

	public static void main(String[] args) throws Exception {
		Server server = new Server(4).bind("tcp://*:5570");

		Client client1 = new Client("client-#1").connect("tcp://localhost:5570");
		Client client2 = new Client("client-#2").connect("tcp://localhost:5570");
		Client client3 = new Client("client-#3").connect("tcp://localhost:5570");

		Thread.sleep(10000);

		client1.destroy();
		client2.destroy();
		client3.destroy();
		server.destroy();
	}

}

