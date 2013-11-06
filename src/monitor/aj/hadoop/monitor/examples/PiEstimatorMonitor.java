package aj.hadoop.monitor.examples;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.lang.management.*;
import org.apache.commons.io.*;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;

import org.apache.hadoop.examples.*;
import org.apache.hadoop.examples.PiEstimator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


@Aspect
public class PiEstimatorMonitor {

    //private static String LOGDIR = "/hadoop/var/log/trace";
    private static String LOGDIR = "/tmp";
    private static String LOGFILE = "trace.log";
    private static String OUT = LOGDIR + "/" + LOGFILE;

    private static String PID = getProcessId();
    public static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
    private static String TS = now();
    private static String HOSTNAME = getHostname();


    @Pointcut("execution(public void org.apache.hadoop.examples.PiEstimator$PiMapper.map" + 
            "(" + 
            "org.apache.hadoop.io.LongWritable, " + 
            "org.apache.hadoop.io.LongWritable, " + 
            "org.apache.hadoop.mapred.OutputCollector<org.apache.hadoop.io.BooleanWritable, org.apache.hadoop.io.LongWritable>, " + 
            "org.apache.hadoop.mapred.Reporter" +
            ")) " +
            "&& args(inKey, inValue, collector, reporter)")
        public void catch_map_method(LongWritable inKey, LongWritable inValue, 
                OutputCollector collector, Reporter reporter) {}


    // run map method
    @Before(value = "catch_map_method(inKey, inValue, collector, reporter)")
        public void logging_map_method(JoinPoint thisJoinPoint, 
                LongWritable inKey, LongWritable inValue, 
                OutputCollector collector, Reporter reporter) {
            try {
                FileOutputStream fos = new FileOutputStream(OUT, true);
                OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
                String ret = "";
                ret = TS + ", " + HOSTNAME + ", ";
                ret += PID + ", ";
                ret += thisJoinPoint + ", ";
                ret += inKey;
                out.write(ret);
                out.write("\n");
                out.close();
            } catch(IOException e) {
                System.out.println(e);
            }
        }


    //--------------
    //  Util
    //--------------

    /**
     * get process id.
     * @args
     * @return String pid
     */
    private static String getProcessId() {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        String vmName = bean.getName();
        String pid = vmName.split("@")[0];
        return pid;
    }


    /**
     * get timestamp
     * @args
     * @return  String timestamp
     */
    public static String now() {
        Date date = new Date();
        return dayFormat.format(date);
    }

    /**
     * get hostname
     * @args
     * @return String hostname
     */
    public static String getHostname() {
        try { return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            //e.printStakTrace();
        }
        return "UnknownHost";
    }
}

    /*
    @Pointcut("execution(* org.apache.hadoop.examples..*.*(..))")
        public void allmethod() {}

    @Before(value = "allmethod()")
        public void logging() {
            System.out.println("** [logging] method execute");
        }
    */
