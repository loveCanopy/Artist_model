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
public class MV {

	
	public static final String TAB = "\001";
	
	 //判断数据是否为空
	public static boolean isKong(String value){
		if(value==null||value.matches("\\[\"\"\\]")||value.matches("\\[\"\",\"\"\\]")){
			return true;
		}else{
			return false;
		}
		
	}
	//去除[""] \n \r \\n \\r // 空格/空格   去空格
	public static String getCommon(String value){
		
		if(value!=null){
			return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "").replace("\t","").replace("\\t", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "").replace(" ", "").trim();
		}else{
			return null;
		}
		
	}
	
	  //处理爱奇艺时间
		public static String getAiqiyiTime(String value){
			if(value!=null){
			Pattern p=Pattern.compile("\\[\"发布时间：(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replace(",", "");
			}else{
				return value;
			}
		}
		//处理music_station时间
		public static String getMusic_stationTime(String value){
			if(value!=null){
			Pattern p=Pattern.compile("\\[\"发布于(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replace(",", "");
			}else{
				return value;
			}
		}
		
		//处理b站 粉丝
		public static String getBilibiliFans(String value){
			if(value!=null){
			Pattern p=Pattern.compile("\\[\"粉丝：(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replace(",", "");
			}else{
				return value;
			}
		}
		
		//处理b站 投稿
		public static String getBilibiliTougao(String value){
			if(value!=null){
			Pattern p=Pattern.compile("\\[\"投稿：(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replace(",", "");
			}else{
				return value;
			}
		}
		//处理优酷评论数
		public static String getYouku_comment(String value){
			if(value!=null){
			Pattern p=Pattern.compile("\\[\"(.*)次评论\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replace(",", "");
			}else{
				return value;
			}
		}
	public static String getCount(String value){
		if(value!=null){
			return value.replace(",", "").replace("\"", "").replace("[", "").replace("]", "").replace("\n","").replace("\\n", "").replace("\t", "").trim();
		}else{
			return null;
		}
		
	}
	public static class MvMapper extends Mapper<LongWritable, Text, Text, Text>{
		   
		
		
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
		    	        //处理A站 乐视 qq_mv xiami_mv
		    		   if(json.getString("site_name").matches(".*acfun.*")||json.getString("site_name").matches(".*letv.*")
		    				   ||json.getString("site_name").matches(".*qq_mv.*")||json.getString("site_name").matches(".*xiami_mv.*")){
				    	   //艺人名
				    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
				    	   //更新时间
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   //mv名称
				    	   String mv_name=isKong(json.getString("mv_name"))?"null":getCommon(json.getString("mv_name"));
				    	   //播放量
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
				    	   //收藏量
				    	   String collect_counts=isKong(json.getString("collect_counts"))?"null":getCount(json.getString("collect_counts"));
				    	   //点赞量
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   //下载量
				    	   String download_counts=isKong(json.getString("download_counts"))?"null":getCount(json.getString("download_counts"));
				    	   //发布时间
				    	   String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
				    	   //评论量
				    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
				    	   //MV_ID
				    	   String mv_id=isKong(json.getString("mv_id"))?"null":getCommon(json.getString("mv_id"));
				    	   //分类
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   //投蕉量、硬币量
				    	   String reward_counts=isKong(json.getString("reward_counts"))?"null":getCount(json.getString("reward_counts"));
				    	   //简介
				    	   String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
				    	   //发布者
				    	   String publish_name=isKong(json.getString("publish_name"))?"null":getCommon(json.getString("publish_name"));
				    	   //发布者id
				    	   String publish_id=isKong(json.getString("publish_id"))?"null":getCommon(json.getString("publish_id"));
				    	   //投稿数
				    	   String sub_counts=isKong(json.getString("sub_counts"))?"null":getCount(json.getString("sub_counts"));
				    	   //发布粉丝数
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
				    	   //充电人数
				    	   String charge_counts=isKong(json.getString("charge_counts"))?"null":getCount(json.getString("charge_counts"));
				    	   //弹幕量
				    	   String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
				   
				    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+mv_name+TAB+play_counts+TAB+collect_counts+TAB+fav_counts+TAB+download_counts
				    			   +TAB+publish_time+TAB+comment_counts+TAB+mv_id+TAB+style_classify+TAB+reward_counts+TAB+introduce+TAB+publish_name+TAB+publish_id+
				    			   TAB+sub_counts+TAB+fans_counts+TAB+charge_counts+TAB+barrage_counts);  
				    	   
				    	   if(output.toString().split(TAB)[0].equals("acfun")){
				    		   mos.write("acfun", output, new Text(), "Acfun/");
				    	   }else if(output.toString().split(TAB)[0].equals("letv")){
				    		   mos.write("letv", output, new Text(), "Letv/");
				    	   }else if(output.toString().split(TAB)[0].equals("qq_mv")){
				    		   mos.write("qq", output, new Text(), "QQ/");
				    	   }else if(output.toString().split(TAB)[0].equals("xiami_mv")){
				    		   mos.write("xiami", output, new Text(), "Xiami/");
				    	   }
				       } 
		    		   //处理爱奇艺
		    		   else if(json.getString("site_name").matches(".*aiqiyi.*")){
				    	   
				    	   //艺人名
				    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
				    	   //更新时间
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   //mv名称
				    	   String mv_name=isKong(json.getString("mv_name"))?"null":getCommon(json.getString("mv_name"));
				    	   //播放量
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
				    	   //收藏量
				    	   String collect_counts=isKong(json.getString("collect_counts"))?"null":getCount(json.getString("collect_counts"));
				    	   //点赞量
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   //下载量
				    	   String download_counts=isKong(json.getString("download_counts"))?"null":getCount(json.getString("download_counts"));
				    	   //发布时间
				    	   String publish_time=isKong(json.getString("publish_time"))?"null":getAiqiyiTime(json.getString("publish_time"));
				    	   //评论量
				    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
				    	   //MV_ID
				    	   String mv_id=isKong(json.getString("mv_id"))?"null":getCommon(json.getString("mv_id"));
				    	   //分类
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   //投蕉量、硬币量
				    	   String reward_counts=isKong(json.getString("reward_counts"))?"null":getCount(json.getString("reward_counts"));
				    	   //简介
				    	   String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce")).replace(",", "").trim();
				    	   //发布者
				    	   String publish_name=isKong(json.getString("publish_name"))?"null":getCommon(json.getString("publish_name"));
				    	   //发布者id
				    	   String publish_id=isKong(json.getString("publish_id"))?"null":getCommon(json.getString("publish_id"));
				    	   //投稿数
				    	   String sub_counts=isKong(json.getString("sub_counts"))?"null":getCount(json.getString("sub_counts"));
				    	   //发布粉丝数
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
				    	   //充电人数
				    	   String charge_counts=isKong(json.getString("charge_counts"))?"null":getCount(json.getString("charge_counts"));
				    	   //弹幕量
				    	   String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
				    	  
				    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+mv_name+TAB+play_counts+TAB+collect_counts+TAB+fav_counts+TAB+download_counts
				    			   +TAB+publish_time+TAB+comment_counts+TAB+mv_id+TAB+style_classify+TAB+reward_counts+TAB+introduce+TAB+publish_name+TAB+publish_id+
				    			   TAB+sub_counts+TAB+fans_counts+TAB+charge_counts+TAB+barrage_counts);
				    	   if(output.toString().split(TAB)[0].equals("aiqiyi_mv")){
				    		   mos.write("aiqiyi", output, new Text(), "Aiqiyi/");
				    	   }
				       } 
		    		   //处理b站
		    		   else if(json.getString("site_name").matches(".*bilibili.*")){
				    	   
				    	   //艺人名
				    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
				    	   //更新时间
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   //mv名称
				    	   String mv_name=isKong(json.getString("mv_name"))?"null":getCommon(json.getString("mv_name"));
				    	   //播放量
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
				    	   //收藏量
				    	   String collect_counts=isKong(json.getString("collect_counts"))?"null":getCount(json.getString("collect_counts"));
				    	   //点赞量
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   //下载量
				    	   String download_counts=isKong(json.getString("download_counts"))?"null":getCount(json.getString("download_counts"));
				    	   //发布时间
				    	   String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
				    	   //评论量
				    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
				    	   //MV_ID
				    	   String mv_id=isKong(json.getString("mv_id"))?"null":getCommon(json.getString("mv_id"));
				    	   //分类
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   //投蕉量、硬币量
				    	   String reward_counts=isKong(json.getString("reward_counts"))?"null":getCount(json.getString("reward_counts"));
				    	   //简介
				    	   String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
				    	   //发布者
				    	   String publish_name=isKong(json.getString("publish_name"))?"null":getCommon(json.getString("publish_name"));
				    	   //发布者id
				    	   String publish_id=isKong(json.getString("publish_id"))?"null":getCommon(json.getString("publish_id"));
				    	   //投稿数
				    	   String sub_counts=isKong(json.getString("sub_counts"))?"null":getBilibiliTougao(json.getString("sub_counts"));
				    	   //发布粉丝数
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getBilibiliFans(json.getString("fans_counts"));
				    	   //充电人数
				    	   String charge_counts=isKong(json.getString("charge_counts"))?"null":getCount(json.getString("charge_counts"));
				    	   //弹幕量
				    	   String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
				    	  
				    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+mv_name+TAB+play_counts+TAB+collect_counts+TAB+fav_counts+TAB+download_counts
				    			   +TAB+publish_time+TAB+comment_counts+TAB+mv_id+TAB+style_classify+TAB+reward_counts+TAB+introduce+TAB+publish_name+TAB+publish_id+
				    			   TAB+sub_counts+TAB+fans_counts+TAB+charge_counts+TAB+barrage_counts);
				    	   if(output.toString().split(TAB)[0].equals("bilibili")){
				    		   mos.write("bilibili", output, new Text(), "Bilibili/");
				    	   }
				       } 
		    		   //处理music_station
				       else if(json.getString("site_name").matches(".*music_station.*")){
				    	  
				    	   //艺人名
				    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
				    	   //更新时间
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   //mv名称
				    	   String mv_name=isKong(json.getString("mv_name"))?"null":getCommon(json.getString("mv_name"));
				    	   //播放量
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
				    	   //收藏量
				    	   String collect_counts=isKong(json.getString("collect_counts"))?"null":getCount(json.getString("collect_counts"));
				    	   //点赞量
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   //下载量
				    	   String download_counts=isKong(json.getString("download_counts"))?"null":getCount(json.getString("download_counts"));
				    	   //发布时间
				    	   String publish_time=isKong(json.getString("publish_time"))?"null":getMusic_stationTime(json.getString("publish_time"));
				    	   //评论量
				    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
				    	   //MV_ID
				    	   String mv_id=isKong(json.getString("mv_id"))?"null":getCommon(json.getString("mv_id"));
				    	   //分类
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   //投蕉量、硬币量
				    	   String reward_counts=isKong(json.getString("reward_counts"))?"null":getCount(json.getString("reward_counts"));
				    	   //简介
				    	   String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
				    	   //发布者
				    	   String publish_name=isKong(json.getString("publish_name"))?"null":getCommon(json.getString("publish_name"));
				    	   //发布者id
				    	   String publish_id=isKong(json.getString("publish_id"))?"null":getCommon(json.getString("publish_id"));
				    	   //投稿数
				    	   String sub_counts=isKong(json.getString("sub_counts"))?"null":getCount(json.getString("sub_counts"));
				    	   //发布粉丝数
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
				    	   //充电人数
				    	   String charge_counts=isKong(json.getString("charge_counts"))?"null":getCount(json.getString("charge_counts"));
				    	   //弹幕量
				    	   String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
				    	  
				    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+mv_name+TAB+play_counts+TAB+collect_counts+TAB+fav_counts+TAB+download_counts
				    			   +TAB+publish_time+TAB+comment_counts+TAB+mv_id+TAB+style_classify+TAB+reward_counts+TAB+introduce+TAB+publish_name+TAB+publish_id+
				    			   TAB+sub_counts+TAB+fans_counts+TAB+charge_counts+TAB+barrage_counts);
				    	   if(output.toString().split(TAB)[0].equals("music_station")){
				    		   mos.write("musicstation", output, new Text(), "Musicstation/");
				    	   }
				       }  else if(json.getString("site_name").matches(".*youku.*")){
				    	 
				    	   //艺人名
				    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
				    	   //更新时间
				    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
				    	   //mv名称
				    	   String mv_name=isKong(json.getString("mv_name"))?"null":getCommon(json.getString("mv_name"));
				    	   //播放量
				    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
				    	   //收藏量
				    	   String collect_counts=isKong(json.getString("collect_counts"))?"null":getCount(json.getString("collect_counts"));
				    	   //点赞量
				    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
				    	   //下载量
				    	   String download_counts=isKong(json.getString("download_counts"))?"null":getCount(json.getString("download_counts"));
				    	   //发布时间
				    	   String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
				    	   //评论量
				    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getYouku_comment(json.getString("comment_counts"));
				    	   //MV_ID
				    	   String mv_id=isKong(json.getString("mv_id"))?"null":getCommon(json.getString("mv_id"));
				    	   //分类
				    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
				    	   //投蕉量、硬币量
				    	   String reward_counts=isKong(json.getString("reward_counts"))?"null":getCount(json.getString("reward_counts"));
				    	   //简介
				    	   String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
				    	   //发布者
				    	   String publish_name=isKong(json.getString("publish_name"))?"null":getCommon(json.getString("publish_name"));
				    	   //发布者id
				    	   String publish_id=isKong(json.getString("publish_id"))?"null":getCommon(json.getString("publish_id"));
				    	   //投稿数
				    	   String sub_counts=isKong(json.getString("sub_counts"))?"null":getCount(json.getString("sub_counts"));
				    	   //发布粉丝数
				    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
				    	   //充电人数
				    	   String charge_counts=isKong(json.getString("charge_counts"))?"null":getCount(json.getString("charge_counts"));
				    	   //弹幕量
				    	   String barrage_counts=isKong(json.getString("barrage_counts"))?"null":getCount(json.getString("barrage_counts"));
				    	  
				    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+mv_name+TAB+play_counts+TAB+collect_counts+TAB+fav_counts+TAB+download_counts
				    			   +TAB+publish_time+TAB+comment_counts+TAB+mv_id+TAB+style_classify+TAB+reward_counts+TAB+introduce+TAB+publish_name+TAB+publish_id+
				    			   TAB+sub_counts+TAB+fans_counts+TAB+charge_counts+TAB+barrage_counts);
				    	   if(output.toString().split(TAB)[0].equals("youku_mv")){
				    		   mos.write("youku", output, new Text(), "Youku/");
				    	   }
				       } 
		    		   
		    	   }
			   }
	}   
		   
	

	
	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(MV.class);
		job.setJobName("MV");
		job.setNumReduceTasks(1);
		job.setMapperClass(MvMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "acfun", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "aiqiyi", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "bilibili", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "musicstation", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "qq", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "xiami", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
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
		String out = "/user/work/ArtistModel/"+time+"/MV";
		Path path = new Path(out);
		hdfs.delete(path, true);
		MV.runLoadMapReducue(conf, args[0], new Path(out));
	}

}
