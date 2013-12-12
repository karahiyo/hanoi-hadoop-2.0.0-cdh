package aj.hadoop.monitor.examples;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.lang.*;
import java.lang.management.*;
import java.util.StringTokenizer;
import java.util.concurrent.Phaser;
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

@Aspect
public class WordCount_noCMonitor {

	private String HOST = "127.0.0.1";
	private int PORT = 55000;

	/** log file */
	public static final String LOGFILE = "Hanoi-MethodTraceMonitor.log";

	/**
	 * pointcut for map method
	 * @param word
	 * @param one
	 */
	@Pointcut ("call(void org.apache.hadoop.mapreduce.Mapper.Context.write" +
			"(" +
			"java.lang.Object, " +
			"java.lang.Object" +
			"))" +
			"&& cflow(execution(public void org.apache.hadoop.examples.WordCount_noCombine$TokenizerMapper.map(..)))" +
			"&& args(word, one)")
	public void catch_map_method(Object word, Object one){}

	/**
	 * pointcut for reduce method
	 * @param word
	 * @param values
	 * @param cntext
	 */
	@Pointcut ("execution(void org.apache.hadoop.examples.WordCount_noCombine.IntSumReducer.reduce(..))")
	public void catch_reduce_method(){}


	/**
	 * logging map method using HanoiPicker
	 * @param thisJoinPoint
	 * @param key
	 * @param one
	 */
	@Before ( value = "catch_map_method( key, one )")
	public void logging_map_method( JoinPoint thisJoinPoint,
			Object key,
			Object one) {

		String phase = "MAP";
		try { 
			PickerClient client = new PickerClient();
			client.send(phase + "," + key);
			client.socketClose();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * logging reduce method using HanoiPicker
	 * @param thisJoinPoint
	 * @param key
	 * @param one
	 */
	@Before ( value = "catch_reduce_method()")
	public void logging_reduce_method( JoinPoint thisJoinPoint) {

		String phase = "SHUFFLE";
		try { 
            Object[] params = thisJoinPoint.getArgs();
			PickerClient client = new PickerClient();
			client.send(phase + "," + params[0].toString());
			client.socketClose();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
