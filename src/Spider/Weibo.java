package Spider;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;



public class Weibo {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		if (args.length == 0) {
			System.err.println("Usage: rcfile <in>");
			System.exit(1);
		}
		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred"; 
		}
		conf.set("mapreduce.job.queuename", queue);
		
		FileSystem hdfs = FileSystem.get(conf);
		Date dNow = new Date();  
	     Date dBefore = new Date();
	     Calendar calendar = Calendar.getInstance(); //得到日历
	     calendar.setTime(dNow);//把当前时间赋给日历
	     calendar.add(Calendar.DAY_OF_MONTH, -1);  //设置为前一天
	     dBefore = calendar.getTime();   //得到前一天的时间
	     SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //设置时间格式
	     String time = sdf.format(dBefore);    //格式化前一天
		String out = "/user/work/ArtistModel/"+time+"/Weibo";
		Path path = new Path(out);
		hdfs.delete(path, true);
		Weibo.runLoadMapReducue(conf, args[0], new Path(out));
		
		
	}

	public static String getCommon(String value){
		
		if(value!=null){
			return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "").trim();
		}else{
			return null;
		}
		
	}
	
	//判断数据是否为空
	public static boolean isKong(String value){
		if(value==null||value.matches("\\[\"\"\\]")){
			return true;
		}else{
			return false;
		}
		
	}
	public static final String TAB = "\t";
	public static class WeiboMapper extends Mapper<LongWritable, Text, Text, Text>{
		   @Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			   if(value.toString().matches("\\{.*\\}")){
				   JSONObject json=JSONObject.parseObject(value.toString());
			       String stars_name=isKong(json.getString("stars_name"))?"null":getCommon(json.getString("stars_name"));
				   String weibo_nums=isKong(json.getString("weibo_nums"))?"null":getCommon(json.getString("weibo_nums"));
				   String fans_nums=isKong(json.getString("fans_nums"))?"null":getCommon(json.getString("fans_nums"));
				   context.write(new Text("weibo"), new Text(stars_name+TAB+weibo_nums+TAB+fans_nums));
			   }
		   }
		   
	}
	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Weibo.class);
		job.setJobName("Weibo");
		job.setNumReduceTasks(0);
		job.setMapperClass(WeiboMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}
	
	
}
