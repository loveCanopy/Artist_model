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

public class Media {

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
	
	//处理乐视 评论  play_counts
	public static String getLetvCommentcounts(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"总评论数： (.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result;
			}else{
				return null;
			}
	}
	
	public static String getLetvPlaycounts(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"总播放量： (.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result;
			}else{
				return null;
			}
	}
	
	//处理优酷
	public static String getYoukuCommentcounts(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"评论：(.*)\"\\]");
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
	
	public static String getYoukuPlaycounts(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"总播放数：(.*)\"\\]");
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
	public static class MediaMapper extends Mapper<LongWritable, Text, Text, Text>{
		   
		
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
			    	 //搜狐  芒果 腾讯 土豆
			    	 if(site_name.matches(".*souhu_show.*")||site_name.matches(".*mangguo_show.*")
			    			 ||site_name.matches(".*tencent_show.*")||site_name.matches(".*tudou_show.*")){
				       String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
			    	   String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
			    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
			    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
			    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
			    	   String show_introduce=isKong(json.getString("show_introduce"))?"null":getCommon(json.getString("show_introduce"));
			    	   String show_name=isKong(json.getString("show_name"))?"null":getCommon(json.getString("show_name"));
			    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
			    	   String host=isKong(json.getString("host"))?"null":getCommon(json.getString("host"));
			    	   String ares=isKong(json.getString("ares"))?"null":getCommon(json.getString("ares"));
			    	   String score_level=isKong(json.getString("score_level"))?"null":getCommon(json.getString("score_level"));
			    	   String first_show=isKong(json.getString("first_show"))?"null":getCommon(json.getString("first_show"));
			    	   String tv_station=isKong(json.getString("tv_station"))?"null":getCommon(json.getString("tv_station"));
			    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    	   output=new Text(site_name+TAB+comment_counts+TAB+score_counts+TAB+update_time+TAB+play_counts+TAB+fans_counts+
			    			   TAB+show_introduce+TAB+show_name+TAB+style_classify+TAB+host+TAB+
			    			   ares+TAB+score_level+TAB+first_show+TAB
			    			   +tv_station+TAB+fav_counts);
			    	   if(output.toString().split(TAB)[0].equals("souhu_show")){
			    		   mos.write("souhu", output, new Text(), "Souhu/");
			    	   }else if(output.toString().split(TAB)[0].equals("mangguo_show")){
			    		   mos.write("mangguo", output, new Text(), "Mangguo/");
			    	   }else if(output.toString().split(TAB)[0].equals("tudou_show")){
			    		   mos.write("tudou", output, new Text(), "Tudou/");
			    	   }else if(output.toString().split(TAB)[0].equals("tencent_show")){
			    		   mos.write("tencent", output, new Text(), "Tencent/");
			    	   }
			    	   
			    	 }
			    	 //乐视
			    	 else if(site_name.matches(".*letv_show.*")){
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getLetvCommentcounts(json.getString("comment_counts"));
				    	   String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getLetvPlaycounts(json.getString("play_counts"));
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
				    	   String show_introduce=isKong(json.getString("show_introduce"))?"null":getCommon(json.getString("show_introduce"));
				    	   String show_name=isKong(json.getString("show_name"))?"null":getCommon(json.getString("show_name"));
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   String host=isKong(json.getString("host"))?"null":getCommon(json.getString("host"));
				    	   String ares=isKong(json.getString("ares"))?"null":getCommon(json.getString("ares"));
				    	   String score_level=isKong(json.getString("score_level"))?"null":getCommon(json.getString("score_level"));
				    	   String first_show=isKong(json.getString("first_show"))?"null":getCommon(json.getString("first_show"));
				    	   String tv_station=isKong(json.getString("tv_station"))?"null":getCommon(json.getString("tv_station"));
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   output=new Text(site_name+TAB+comment_counts+TAB+score_counts+TAB+update_time+TAB+play_counts+TAB+fans_counts+
				    			   TAB+show_introduce+TAB+show_name+TAB+style_classify+TAB+host+TAB+
				    			   ares+TAB+score_level+TAB+first_show+TAB
				    			   +tv_station+TAB+fav_counts);
				    	   if(output.toString().split(TAB)[0].equals("letv_show")){
				    		   mos.write("letv", output, new Text(), "Letv/");
				    	   }
			    		 
			    	 //优酷	 
			    	 }else if(site_name.matches(".*youku_show.*")){
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getYoukuCommentcounts(json.getString("comment_counts"));
				    	   String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getYoukuPlaycounts(json.getString("play_counts"));
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
				    	   String show_introduce=isKong(json.getString("show_introduce"))?"null":getCommon(json.getString("show_introduce"));
				    	   String show_name=isKong(json.getString("show_name"))?"null":getCommon(json.getString("show_name"));
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   String host=isKong(json.getString("host"))?"null":getCommon(json.getString("host"));
				    	   String ares=isKong(json.getString("ares"))?"null":getCommon(json.getString("ares"));
				    	   String score_level=isKong(json.getString("score_level"))?"null":getCommon(json.getString("score_level"));
				    	   String first_show=isKong(json.getString("first_show"))?"null":getCommon(json.getString("first_show"));
				    	   String tv_station=isKong(json.getString("tv_station"))?"null":getCommon(json.getString("tv_station"));
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   output=new Text(site_name+TAB+comment_counts+TAB+score_counts+TAB+update_time+TAB+play_counts+TAB+fans_counts+
				    			   TAB+show_introduce+TAB+show_name+TAB+style_classify+TAB+host+TAB+
				    			   ares+TAB+score_level+TAB+first_show+TAB
				    			   +tv_station+TAB+fav_counts);
				    	   if(output.toString().split(TAB)[0].equals("youku_show")){
				    		   mos.write("youku", output, new Text(), "Youku/");
				    	   }
			    		 
			    	 }
			    	 
			    	 
			    	 
			   }
		   }
	
	
	}   
	

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Media.class);
		job.setJobName("Media");
		job.setNumReduceTasks(1);
		job.setMapperClass(MediaMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "tencent", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "mangguo", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class,
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
		String out = "/user/work/ArtistModel/"+time+"/Media";
		Path path = new Path(out);
		hdfs.delete(path, true);
		Media.runLoadMapReducue(conf, args[0], new Path(out));
	}

}

