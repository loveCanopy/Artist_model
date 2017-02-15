package Spider;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSONObject;




public class Media_sp {

public static String getCommon(String value){
		
		if(value!=null){
			return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "").replace(" ", "").trim();
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
	
	public static String getCount(String value){
		if(value!=null){
			return value.replace(",", "").replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\\n", "").trim();
		}else{
			return null;
		}
		
	}
	
	//搜狐评论 特殊处理
	public static String getSouhuCommentcounts(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"\\((.*)人参与，(.*)条评论\\)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(2);
			}
			return result.replace(",", "");
			}else{
				return null;
			}
	}
	//优酷评论 特殊处理
	public static String getYoukuCommentcounts(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"(.*)次评论\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replace(",", "");
			}else{
				return null;
			}
	}
	
	
	
	public static final String TAB = "\001";
	public static class MediaspMapper extends Mapper<LongWritable, Text, Text, Text>{
		   
		//将结果输出到多个文件或多个文件夹  
	    public MultipleOutputs<Text,Text> mos;  
	    //创建MultipleOutputs对象  
	    public void setup(Context context) throws IOException,InterruptedException {  
	        mos = new MultipleOutputs<Text, Text>(context);  
	     }  
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			   
			   Text output;
			   if(value.toString().matches("\\{.*\\}")){
				   JSONObject json=JSONObject.parseObject(value.toString());
					 //网站标示
			    	 String site_name=isKong(json.getString("site_name"))?"null":getCommon(json.getString("site_name"));
			    	 if(site_name.matches(".*letv_sp.*")||site_name.matches(".*mangguo_sp.*")||site_name.matches(".*tencent_sp.*")){
			    		 String show_name=isKong(json.getString("show_name"))?"null":getCommon(json.getString("show_name"));
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 String host=isKong(json.getString("host"))?"null":getCommon(json.getString("host"));
			    		 String guest=isKong(json.getString("guest"))?"null":getCommon(json.getString("guest"));
			    		 String sp_plays=isKong(json.getString("sp_plays"))?"null":getCount(json.getString("sp_plays"));
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
			    		 String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 String discuss_counts=isKong(json.getString("discuss_counts"))?"null":getCount(json.getString("discuss_counts"));
			    		 String vote_counts=isKong(json.getString("vote_counts"))?"null":getCount(json.getString("vote_counts"));
			    		 output= new Text(site_name+TAB+show_name+TAB+publish_time+TAB+host+TAB+guest+TAB+sp_plays+TAB+comment_counts+TAB+
			    				 barrage_counts+TAB+fav_counts+TAB+step_counts+TAB+discuss_counts+TAB+vote_counts);
			    		 
			    		 if(output.toString().split(TAB)[0].equals("letv_sp")){
				    		   mos.write("letv", output, new Text(), "Letv/");
				    	   }else if(output.toString().split(TAB)[0].equals("mangguo_sp")){
				    		   mos.write("mangguo", output, new Text(), "Mangguo/");
				    	   }else if(output.toString().split(TAB)[0].equals("tencent_sp")){
				    		   mos.write("tencent", output, new Text(), "Tencent/");
				    	   }
			    		 
			    	 }else if(site_name.matches(".*souhu_sp.*")){
			    		 String show_name=isKong(json.getString("show_name"))?"null":getCommon(json.getString("show_name")).trim().replace("\t","").replace(",", "");
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 String host=isKong(json.getString("host"))?"null":getCommon(json.getString("host"));
			    		 String guest=isKong(json.getString("guest"))?"null":getCommon(json.getString("guest"));
			    		 String sp_plays=isKong(json.getString("sp_plays"))?"null":getCount(json.getString("sp_plays"));
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getSouhuCommentcounts(json.getString("comment_counts"));
			    		 String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 String discuss_counts=isKong(json.getString("discuss_counts"))?"null":getCount(json.getString("discuss_counts"));
			    		 String vote_counts=isKong(json.getString("vote_counts"))?"null":getCount(json.getString("vote_counts"));
			    		 output= new Text(site_name+TAB+show_name+TAB+publish_time+TAB+host+TAB+guest+TAB+sp_plays+TAB+comment_counts+TAB+
			    				 barrage_counts+TAB+fav_counts+TAB+step_counts+TAB+discuss_counts+TAB+vote_counts);
			    		 if(output.toString().split(TAB)[0].equals("souhu_sp")){
				    		   mos.write("souhu", output, new Text(), "Souhu/");
				    	   }
			    	 }else if(site_name.matches(".*youku_sp.*")){
			    		 
			    		 String show_name=isKong(json.getString("show_name"))?"null":getCommon(json.getString("show_name"));
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 String host=isKong(json.getString("host"))?"null":getCommon(json.getString("host"));
			    		 String guest=isKong(json.getString("guest"))?"null":getCommon(json.getString("guest"));
			    		 String sp_plays=isKong(json.getString("sp_plays"))?"null":getCount(json.getString("sp_plays"));
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getYoukuCommentcounts(json.getString("comment_counts"));
			    		 String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 String discuss_counts=isKong(json.getString("discuss_counts"))?"null":getCount(json.getString("discuss_counts"));
			    		 String vote_counts=isKong(json.getString("vote_counts"))?"null":getCount(json.getString("vote_counts"));
			    		 output= new Text(site_name+TAB+show_name+TAB+publish_time+TAB+host+TAB+guest+TAB+sp_plays+TAB+comment_counts+TAB+
			    				 barrage_counts+TAB+fav_counts+TAB+step_counts+TAB+discuss_counts+TAB+vote_counts);
			    		 if(output.toString().split(TAB)[0].equals("youku_sp")){
				    		   mos.write("youku", output, new Text(), "Youku/");
				    	   }
				    	   
			    	 }
			    	 
			    	 
			    	 
                      			   
			   }
			   
		   }
		   
	} 
	

	
	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Media_sp.class);
		job.setJobName("Media_SP");
		job.setNumReduceTasks(1);
		job.setMapperClass(MediaspMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "tencent", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "mangguo", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class,
                Text.class, NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}
	
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
		String out = "/user/work/ArtistModel/"+time+"/Media_sp";
		Path path = new Path(out);
		hdfs.delete(path, true);
		Media_sp.runLoadMapReducue(conf, args[0], new Path(out));
		
	}

}
