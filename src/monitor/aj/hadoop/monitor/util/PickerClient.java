package com.github.karahiyo.hanoi.picker_client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
	DatagramSocket sendSocket = null;
	InetAddress inetAddress = null;

	/**
	 * initialize
	 */
	public PickerClient() {

		try {
			sendSocket = new DatagramSocket(this.PORT);
		} catch (SocketException e) {
			e.printStackTrace();
		}

		/** setup send host */
		try {
			inetAddress = InetAddress.getByName("127.0.0.1");
		} catch ( UnknownHostException uhe) {
			System.err.println("Don't know about host: " + this.HOST + ":" + this.PORT);
		} catch ( IOException ioe) {
			System.err.println("Could't get I/O for the connection to: " + this.HOST + ":" + this.PORT);
		}

	}

	public void setHost(String host){
		this.HOST = host;
	}

	public void setPORT(int port) {
		this.PORT = port;
	}

	public boolean send(String msg) {
		// メッセージの送信
		byte[] buf = null;
		try {
			buf = msg.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		if(buf == null || msg == null) {
			return false;
		}
		DatagramPacket packet = new DatagramPacket(buf, buf.length, inetAddress, this.PORT);
		try {
			sendSocket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	public boolean socketClose() {
		sendSocket.close();
		return true;
	}
}
