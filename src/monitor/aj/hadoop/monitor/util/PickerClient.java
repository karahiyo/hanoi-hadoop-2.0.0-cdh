package aj.hadoop.monitor.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;

public class PickerClient {

	private int PORT = 9999;
	
	private String HOST = "localhost";

	/** server socket timeout(ms) */
    public static final int TIMEOUT_SERVER_SOCKET     = 500;
    
    public void setHost(String host){
    	this.HOST = host;
    }
    
    public void setPORT(int port) {
    	this.PORT = port;
    }
    
	public void send(String msg) {
		// ソケットや入出力用のストリームの宣言
		Socket echoSocket = null;
		DataOutputStream os = null;
		BufferedReader is = null;

		// Socketの準備
		try {
			echoSocket = new Socket(this.HOST, this.PORT);
			echoSocket.setSoTimeout(TIMEOUT_SERVER_SOCKET);		
			os = new DataOutputStream( echoSocket.getOutputStream());
			is = new BufferedReader( new InputStreamReader(echoSocket.getInputStream()));
		} catch ( UnknownHostException e) {
			System.err.println("Don't know about host: " + this.HOST + ":" + this.PORT);
		} catch ( IOException e) {
			System.err.println("Could't get I/O for the connection to: " + this.HOST + ":" + this.PORT);
		}

		// テストメッセージの送信
		if ( echoSocket != null && os != null && is != null) {
			try {
				// メッセージの送信
				String message = URLEncoder.encode(msg, "UTF-8");
				os.writeBytes(message + "\n");

				// サーバーからのメッセージを受け取り、出力
				String response;
				if ( (response = is.readLine()) != null) {
					System.out.println( "Server: " + response);
				}

				// 開いたソケットをクローズ
				os.close();
				is.close();
				echoSocket.close();
			} catch (UnknownHostException e) {
				System.err.println("Trying to connect to unknown host: " + e);
			} catch (IOException e) {
				System.err.println("IOException: " + e);
			}
		}
	}

}
