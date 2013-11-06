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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.Signature;

@Aspect
public class WordSearchMonitor {

    private static String LOGDIR = "/hadoop/var/log/trace";
    private static String MONITOR_TARGET = "wordsearch";
    private static String LOGFILE = "trace.log";
    private static String OUT = LOGDIR + "/" + MONITOR_TARGET + "-" + LOGFILE;

    private static String PID = getProcessId();
    public static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
    private static String TS = now();
    private static String HOSTNAME = getHostName();

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
            try {
                String out_file = LOGDIR + "/" + LOGFILE;
                FileOutputStream fos = new FileOutputStream(out_file, true);
                OutputStreamWriter out = new OutputStreamWriter(fos, "US-ASCII");
                String ret = "";

                // timestamp
                ret = "time:" + TS + "\t"; 

                // hostname
                ret += "host:" + HOSTNAME + "\t";

                // pid
                ret += "pid:" + PID + "\t";

                // mapper output key
                ret += "key:" + key + "\t";

                // get thisJoinPoint target object = Mapper.Context
                Context context = (Context)thisJoinPoint.getTarget();

                // TaskAttemptID
                // >> 
                // TaskAttemptID represents the immutable and 
                // unique identifier for a task attempt. 
                // Each task attempt is one particular instance of 
                // a Map or Reduce Task identified by its TaskID. 
                // TaskAttemptID consists of 2 parts. 
                // First part is the TaskID, that this TaskAttemptID belongs to. 
                // Second part is the task attempt number. 
                // An example TaskAttemptID is : 
                // attempt_200707121733_0003_m_000005_0 , 
                // which represents the zeroth task attempt for the fifth map task 
                // in the third job running at the jobtracker 
                // started at 200707121733.
                TaskAttemptID taskId = (TaskAttemptID)context.getTaskAttemptID();
                ret += "task-id:" + taskId + "\t";
                //ret += taskId.getTaskID() + "\t";
                
                // InputSplit 
                // - InputSplit represents the data to be processed 
                // by an individual Mapper.
                ret += "split-id:" + context.getInputSplit() + "\t";

                // In here, currentKey mean row number of input file.
                ret += "#rows:" + context.getCurrentKey() + "\t";

                //ret += context.getCurrentValue() + "\t";
                ret += "\n";
                out.write(ret);
                out.close();
            } catch (IOException ioe) {
                System.out.println(ioe);
            } catch (Exception e) {
                System.out.println(e);
            }
            // } catch (InterruptedException ie) {
            //    System.out.println(ie);

        }

    //-------------
    // utils
    //-------------

    /**
     * get pid
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
     * @return String timestamp
     */
    public static String now() {
        //long now = System.currentTimeMillis();
        //return Long.toString(now);
        Date date = new Date();
        return dayFormat.format(date);
    }

    /**
     * get hostname
     * @args
     * @return String hostname
     */
    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            // e.printStackTrace();
        }
        return "unknown";
    }
}
