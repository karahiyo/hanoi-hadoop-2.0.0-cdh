package aj.hadoop.monitor.examples;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.lang.*;
import java.lang.management.*;
import java.util.StringTokenizer;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;

import aj.hadoop.monitor.util.PickerClient;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.FileHandler;

@Aspect
public class WordSearchMonitor {

	private String HOST = "localhost";
	private int PORT = 9999;
	
	/** log file */
	public static final String LOGFILE = "Hanoi-MethodTraceMonitor.log";
	
	/** logger */
	private Logger logger = null;
	
	/** file handler */
	private FileHandler fh = null;
	
	/**
	 * initialize
	 */
	public WordSearchMonitor() {
		logger = Logger.getLogger(this.getClass().getName());
		try {
			fh = new FileHandler(this.LOGFILE, true);
			logger.addHandler(fh);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		logger.log(Level.INFO, "Start trace...");
	}
	
	/**
	 * pointcut
	 * @param word
	 * @param one
	 */
    @Pointcut ("call(void org.apache.hadoop.mapreduce.Mapper.Context.write" +
            "(" +
            "java.lang.Object, " +
            "java.lang.Object" +
            "))" +
            "&& cflow(execution(public void org.apache.hadoop.examples.WordSearch$TokenizerMapper.map(..)))" +
            "&& args(word, one)")
        public void catch_map_method(Object word, Object one){}

    @Before ( value = "catch_map_method( key, one )")
        public void logging_current_method( JoinPoint thisJoinPoint,
                                            Object key,
                                            Object one) {
    	//PickerClient client = new PickerClient();
    	//client.setHost(this.HOST);
    	//client.setPORT(this.PORT);
    	//client.send((String)key);
    	//System.out.println((String)key);
    	
        String ret = "";
 		try {
 	        String outfile = "/tmp" + "/" + this.LOGFILE;
 	        FileOutputStream fos = new FileOutputStream(outfile, true);
 	        OutputStreamWriter out = new OutputStreamWriter(fos);
             ret += "key:" + (String)key;
 	        out.write(ret);
 	        out.close();
 		} catch (IOException ioe) {
 			System.out.println(ioe);
 		} catch (Exception e) {
 			System.out.println(e);
 		}
 		System.err.println("** [POINTCUT]" + ret);
 		System.out.println("** [POINTCUT]" + ret);
    }
}