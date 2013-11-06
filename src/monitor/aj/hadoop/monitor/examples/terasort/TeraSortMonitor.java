package aj.hadoop.monitor.examples.terasort;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;
import java.lang.management.*;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;

import org.apache.hadoop.examples.terasort.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


@Aspect
public class TeraSortMonitor {

    private static String LOGDIR = "/hadoop/var/log/trace";
    private static String LOGFILE = "trace.log";
    private static String OUT = LOGDIR + "/" + LOGFILE;

    private static String PID = getProcessId();
    public static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
    private static String TS = now();
    private static String HOSTNAME = getHostname();



    @Pointcut("execution(int org.apache.hadoop.examples.terasort.TeraSort.TotalOrderPartitioner.getPartition(" + 
            "org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, int" + 
            ")) " +
            "&& args(key, value, numPartitions)")
        public void catch_map_method(Text key, Text value, int numPartitions) {}


    // run map method
    @Before(value = "catch_map_method(key, value, numPartitions)")
        public void logging_map_method(JoinPoint thisJoinPoint, 
                Text key, Text value, int numPartitions) {
            try {
                FileOutputStream fos = new FileOutputStream(OUT, true);
                OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
                String ret = "";
                ret = TS + ", " + HOSTNAME + ", ";
                ret += PID + ", ";
                ret += thisJoinPoint + ", ";
                ret += key;
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

