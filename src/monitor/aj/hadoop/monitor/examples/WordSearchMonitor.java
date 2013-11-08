package aj.hadoop.monitor.examples;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.lang.*;
import java.lang.management.*;
import java.util.StringTokenizer;

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
public class WordSearchMonitor {

	private String HOST = "localhost";
	private int PORT = 9999;
	
    @Pointcut ("call(void org.apache.hadoop.mapreduce.Mapper.Context.write" +
            "(" +
            "java.lang.Object, " +
            "java.lang.Object" +
            "))" +
            "&& cflow(execution(public void org.apache.hadoop.examples.WordSearch$TokenizerMapper.map(..)))" +
            "&& args(word, one)")
        public void catch_map_method(Object word, Object one){}

    @Before ( value = "catch_map_method( word, one )")
        public void logging_current_method( JoinPoint thisJoinPoint,
                                            Object word,
                                            Object one) {
    	PickerClient client = new PickerClient();
    	client.setHost(this.HOST);
    	client.setPORT(this.PORT);
    	client.send((String)word);
    	System.out.println((String)word);
    }
}