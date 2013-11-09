package aj.hadoop.monitor.util;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class PickerClient {

	/** host name */
	private String HOST = "localhost";

	/** port */
	private int PORT = 55000;

	/** server socket timeout(ms) */
	public static final int TIMEOUT_SERVER_SOCKET     = 500;

	/** UDP socket for send */
	DatagramSocket sendSocket = new DatagramSocket();

	/** setup send host */
	InetAddress inetAddress = InetAddress.getByName("127.0.0.1");

	/**
	 * initialize
	 */
	public PickerClient() throws SocketException, UnknownHostException{
	}

	public void setHost(String host){
		this.HOST = host;
	}

	public void setPORT(int port) {
		this.PORT = port;
	}

	public boolean send(String msg) throws Exception{
		// メッセージの送信
		byte[] buf  = msg.getBytes("UTF-8");
		DatagramPacket packet = new DatagramPacket(buf, buf.length, inetAddress, this.PORT);
		sendSocket.send(packet);
		return true;
	}

	public boolean socketClose() {
		sendSocket.close();
		return true;
	}
}
