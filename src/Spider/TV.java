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





public class TV {

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
		String out = "/user/work/ArtistModel/"+time+"/tv";
		Path path = new Path(out);
		hdfs.delete(path, true);
		TV.runLoadMapReducue(conf, args[0], new Path(out));
	}

	public static String TAB="\001";
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
	
	//乐视tv_counts
	public static String getLetv_counts(String value){
		
		if(value!=null){
			Pattern p=Pattern.compile("\\[\".*共(.*)集\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "");
			}else{
				return null;
			}
		
	}
	
	
	//乐视update_counts
	public static String getLetv_updatecounts(String value){
		
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"更新至(.*)集 / 共.*\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "");
			}else{
				return null;
			}
		
	}
	
	//搜狐 tv_counts
	public static String getSouhu_tvcounts(String value){
		
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"(.*)集全\"\\]");
			Pattern p1=Pattern.compile("\\[\".*\",\"/(.*)集 周.*\"\\]");
			Matcher m=p.matcher(value);
			Matcher m1=p1.matcher(value);
			String result="";
			while(m.find()){
			 result=m.group(1);
			}
			
			while(m1.find()){
			 result= m1.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "");
			}else{
				return null;
			}
		
	}
	
	//优酷的评论数
	public static String getYouku_comment(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"评论：(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "").replace(",", "");
			}else{
				return null;
			}
	}
	
	//优酷的总播放数
	public static String getYouku_Play(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"总播放数：(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "").replace(",", "");
			}else{
				return null;
			}
	}
	
	
	//优酷的顶数
	
	public static String getYouku_Fav(String value){
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"顶：(.*)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "").replace(",", "");
			}else{
				return null;
			}
	}
	
	//优酷集数	
	public static String getYouku_Tvcount(String value){
	if(value!=null){
		Pattern p=Pattern.compile("\\[\"(.*)集全\"\\]");
		Matcher m=p.matcher(value);
		String result="";
		while(m.find()){
		result= m.group(1);
		}
		return result.replaceAll("\\\\n", "").replaceAll("\"", "");
		}else{
			return null;
		}
}
	//豆瓣 publish_time
	public static String getDouban_publishtime(String value){
		
		return value.replace(",", "").replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\\n", "").replace("(", "").
				replace(")", "").trim();
	}
	//得到豆瓣的firstplay_time
	public static String getDouban_firstplaytime(String value){
		
		if(value!=null){
			Pattern p=Pattern.compile("\\[\"(.*)\\((.*)\\)\"\\]");
			Matcher m=p.matcher(value);
			String result="";
			while(m.find()){
			result= m.group(1);
			}
			return result.replaceAll("\\\\n", "").replaceAll("\"", "");
			}else{
				return null;
			}
	}
	
	
	//得到豆瓣的area
	public static String getDouban_area(String value){
		
	if(value!=null){
		Pattern p=Pattern.compile("\\[\"(.*)\\((.*)\\)\"\\]");
		Matcher m=p.matcher(value);
		String result="";
		while(m.find()){
		result= m.group(2);
		}
		return result.replaceAll("\\\\n", "").replaceAll("\"", "");
		}else{
			return null;
		}
	
	}
	//得到豆瓣的tv_counts
	public static String getDouban_tvcounts(String value){
		  String[] values=getCommon(value).split(",");
		   String tv_counts=null;
		   for(int i=0;i<values.length;i++){
			  
			   if(values[i].matches(" [0-9]+")){
				   tv_counts=  values[i].trim();
			   } 
			   
		   }
		   return tv_counts;
	}
	
	//得到豆瓣的语言
	public static String getDouban_language(String value){
		   String[] values=getCommon(value).split(",");
		   String language=null;
		   for(int i=0;i<values.length;i++){
			  
			   if(values[i].matches(".*语.*")){
				    language=  values[i].trim();
			   } 
			   
		   }
		   return language;
	}
	
	public static class TvMapper extends Mapper<LongWritable, Text, Text, Text>{
		   
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
			    	 //乐视
			    	 if(site_name.matches(".*letv_tv.*")){
			    		 //电视剧名称
			    		 String tv_name=isKong(json.getString("tv_name"))?"null":getCommon(json.getString("tv_name"));
			    		 //集数
			    		 String tv_counts=isKong(json.getString("tv_counts"))?"null":getLetv_counts(json.getString("tv_counts"));
			    		 //更新集数
			    		 String update_counts=isKong(json.getString("update_counts"))?"null":getLetv_updatecounts(json.getString("update_counts"));
			    		 //上映时间
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 //开播时间
			    		 String firstplay_time=isKong(json.getString("firstplay_time"))?"null":getCommon(json.getString("firstplay_time"));
			    		 //评分
			    		 String score_level=isKong(json.getString("score_level"))?"null":getCommon(json.getString("score_level"));
			    		 //评分人数
			    		 String score_counts=isKong(json.getString("score_counts"))?"null":getCommon(json.getString("score_counts"));
			    		 //主演
			    		 String primary_stars=isKong(json.getString("primary_stars"))?"null":getCommon(json.getString("primary_stars"));
			    		 //导演
			    		 String director=isKong(json.getString("director"))?"null":getCommon(json.getString("director"));
			    		 //类型
			    		 String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
			    		 //地区
			    		 String area=isKong(json.getString("area"))?"null":getCommon(json.getString("area"));
			    		 //总播放量
			    		 String play_counts=isKong(json.getString("play_counts"))?"null":getCommon(json.getString("play_counts"));
			    		 //评论量
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getCommon(json.getString("comment_counts"));
			    		 //顶数
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCommon(json.getString("fav_counts"));
			    		 //踩量
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCommon(json.getString("step_counts"));
			    		 //简介
			    		 String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
			    		 //编剧
			    		 String screenwriter=isKong(json.getString("screenwriter"))?"null":getCommon(json.getString("screenwriter"));
			    		 //语言
			    		 String language=isKong(json.getString("language"))?"null":getCommon(json.getString("language"));
			    		 output=new Text(site_name+TAB+tv_name+TAB+tv_counts+TAB+update_counts+TAB+publish_time+TAB+firstplay_time+TAB+score_level+TAB+score_counts+TAB+
			    				 primary_stars+TAB+director+TAB+style_classify+TAB+area+TAB+play_counts+TAB+comment_counts+TAB+fav_counts+TAB+step_counts+TAB+introduce+TAB+screenwriter+TAB+language);
			    		 if(output.toString().split(TAB)[0].equals("letv_tv")){
				    		   mos.write("letv", output, new Text(), "Letv/");
				    	   }
			    		 
			    		 //芒果 土豆 
			    	 }else if(site_name.matches(".*mangguo_tv.*")||site_name.matches(".*tudou_tv.*")){
			    		 //电视剧名称
			    		 String tv_name=isKong(json.getString("tv_name"))?"null":getCommon(json.getString("tv_name"));
			    		 //集数
			    		 String tv_counts=isKong(json.getString("tv_counts"))?"null":getCount(json.getString("tv_counts"));
			    		 //更新集数
			    		 String update_counts=isKong(json.getString("update_counts"))?"null":getCount(json.getString("update_counts"));
			    		 //上映时间
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 //开播时间
			    		 String firstplay_time=isKong(json.getString("firstplay_time"))?"null":getCommon(json.getString("firstplay_time"));
			    		 //评分
			    		 String score_level=isKong(json.getString("score_level"))?"null":getCount(json.getString("score_level"));
			    		 //评分人数
			    		 String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
			    		 //主演
			    		 String primary_stars=isKong(json.getString("primary_stars"))?"null":getCommon(json.getString("primary_stars"));
			    		 //导演
			    		 String director=isKong(json.getString("director"))?"null":getCommon(json.getString("director"));
			    		 //类型
			    		 String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
			    		 //地区
			    		 String area=isKong(json.getString("area"))?"null":getCommon(json.getString("area"));
			    		 //总播放量
			    		 String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
			    		 //评论量
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
			    		 //顶数
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    		 //踩量
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 //简介
			    		 String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
			    		 //编剧
			    		 String screenwriter=isKong(json.getString("screenwriter"))?"null":getCommon(json.getString("screenwriter"));
			    		 //语言
			    		 String language=isKong(json.getString("language"))?"null":getCommon(json.getString("language"));
			    		 output=new Text(site_name+TAB+tv_name+TAB+tv_counts+TAB+update_counts+TAB+publish_time+TAB+firstplay_time+TAB+score_level+TAB+score_counts+TAB+
			    				 primary_stars+TAB+director+TAB+style_classify+TAB+area+TAB+play_counts+TAB+comment_counts+TAB+fav_counts+TAB+step_counts+TAB+introduce+TAB+screenwriter+TAB+language);
			    		 if(output.toString().split(TAB)[0].equals("mangguo_tv")){
				    		   mos.write("mangguo", output, new Text(), "Mangguo/");
				    	   }else if(output.toString().split(TAB)[0].equals("tudou_tv")){
				    		   mos.write("tudou", output, new Text(), "Tudou/");
				    	   }
			    		 //搜狐
			    	 }else if(site_name.matches(".*souhu_tv.*")){
			    		 //电视剧名称
			    		 String tv_name=isKong(json.getString("tv_name"))?"null":getCommon(json.getString("tv_name"));
			    		 //集数
			    		 String tv_counts=isKong(json.getString("tv_counts"))?"null":getSouhu_tvcounts(json.getString("tv_counts"));
			    		 //更新集数
			    		 String update_counts=isKong(json.getString("update_counts"))?"null":getCount(json.getString("update_counts"));
			    		 //上映时间
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 //开播时间
			    		 String firstplay_time=isKong(json.getString("firstplay_time"))?"null":getCommon(json.getString("firstplay_time"));
			    		 //评分
			    		 String score_level=isKong(json.getString("score_level"))?"null":getCount(json.getString("score_level"));
			    		 //评分人数
			    		 String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
			    		 //主演
			    		 String primary_stars=isKong(json.getString("primary_stars"))?"null":getCommon(json.getString("primary_stars"));
			    		 //导演
			    		 String director=isKong(json.getString("director"))?"null":getCommon(json.getString("director"));
			    		 //类型
			    		 String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
			    		 //地区
			    		 String area=isKong(json.getString("area"))?"null":getCommon(json.getString("area"));
			    		 //总播放量
			    		 String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
			    		 //评论量
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
			    		 //顶数
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    		 //踩量
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 //简介
			    		 String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
			    		 //编剧
			    		 String screenwriter=isKong(json.getString("screenwriter"))?"null":getCommon(json.getString("screenwriter"));
			    		 //语言
			    		 String language=isKong(json.getString("language"))?"null":getCommon(json.getString("language"));
			    		 output=new Text(site_name+TAB+tv_name+TAB+tv_counts+TAB+update_counts+TAB+publish_time+TAB+firstplay_time+TAB+score_level+TAB+score_counts+TAB+
			    				 primary_stars+TAB+director+TAB+style_classify+TAB+area+TAB+play_counts+TAB+comment_counts+TAB+fav_counts+TAB+step_counts+TAB+introduce+TAB+screenwriter+TAB+language);
			    		 if(output.toString().split(TAB)[0].equals("souhu_tv")){
				    		   mos.write("souhu", output, new Text(), "Souhu/");
			    		 }
			    	 }else if(site_name.matches(".*youku_tv.*")){
			    		 //电视剧名称
			    		 String tv_name=isKong(json.getString("tv_name"))?"null":getCommon(json.getString("tv_name"));
			    		 //集数
			    		 String tv_counts=isKong(json.getString("tv_counts"))?"null":getYouku_Tvcount(json.getString("tv_counts"));
			    		 //更新集数
			    		 String update_counts=isKong(json.getString("update_counts"))?"null":getCount(json.getString("update_counts"));
			    		 //上映时间
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getCommon(json.getString("publish_time"));
			    		 //开播时间
			    		 String firstplay_time=isKong(json.getString("firstplay_time"))?"null":getCommon(json.getString("firstplay_time"));
			    		 //评分
			    		 String score_level=isKong(json.getString("score_level"))?"null":getCount(json.getString("score_level"));
			    		 //评分人数
			    		 String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
			    		 //主演
			    		 String primary_stars=isKong(json.getString("primary_stars"))?"null":getCommon(json.getString("primary_stars"));
			    		 //导演
			    		 String director=isKong(json.getString("director"))?"null":getCommon(json.getString("director"));
			    		 //类型
			    		 String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
			    		 //地区
			    		 String area=isKong(json.getString("area"))?"null":getCommon(json.getString("area"));
			    		 //总播放量
			    		 String play_counts=isKong(json.getString("play_counts"))?"null":getYouku_Play(json.getString("play_counts"));
			    		 //评论量
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getYouku_comment(json.getString("comment_counts"));
			    		 //顶数
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getYouku_Fav(json.getString("fav_counts"));
			    		 //踩量
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 //简介
			    		 String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce"));
			    		 //编剧
			    		 String screenwriter=isKong(json.getString("screenwriter"))?"null":getCommon(json.getString("screenwriter"));
			    		 //语言
			    		 String language=isKong(json.getString("language"))?"null":getCommon(json.getString("language")); 
			    		 output=new Text(site_name+TAB+tv_name+TAB+tv_counts+TAB+update_counts+TAB+publish_time+TAB+firstplay_time+TAB+score_level+TAB+score_counts+TAB+
			    				 primary_stars+TAB+director+TAB+style_classify+TAB+area+TAB+play_counts+TAB+comment_counts+TAB+fav_counts+TAB+step_counts+TAB+introduce+TAB+screenwriter+TAB+language);
			    		 if(output.toString().split(TAB)[0].equals("youku_tv")){
				    		   mos.write("youku", output, new Text(), "Youku/");
			    		 }
			    	 }
			    	 else if (site_name.matches(".*douban_tv.*")){
			    		 
			    		//电视剧名称
			    		 String tv_name=isKong(json.getString("tv_name"))?"null":getCommon(json.getString("tv_name"));
			    		 //集数
			    		 String tv_counts=isKong(json.getString("tv_counts"))?"null":getDouban_tvcounts(json.getString("tv_counts"));
			    		 //更新集数
			    		 String update_counts=isKong(json.getString("update_counts"))?"null":getCount(json.getString("update_counts"));
			    		 //上映时间
			    		 String publish_time=isKong(json.getString("publish_time"))?"null":getDouban_publishtime(json.getString("publish_time"));
			    		 //开播时间
			    		 String firstplay_time=isKong(json.getString("firstplay_time"))?"null":getDouban_firstplaytime(json.getString("firstplay_time"));
			    		 //评分
			    		 String score_level=isKong(json.getString("score_level"))?"null":getCount(json.getString("score_level"));
			    		 //评分人数
			    		 String score_counts=isKong(json.getString("score_counts"))?"null":getCount(json.getString("score_counts"));
			    		 //主演
			    		 String primary_stars=isKong(json.getString("primary_stars"))?"null":getCommon(json.getString("primary_stars"));
			    		 //导演
			    		 String director=isKong(json.getString("director"))?"null":getCommon(json.getString("director"));
			    		 //类型
			    		 String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
			    		 //地区
			    		 String area=isKong(json.getString("area"))?"null":getDouban_area(json.getString("area"));
			    		 //总播放量
			    		 String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
			    		 //评论量
			    		 String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
			    		 //顶数
			    		 String fav_counts=isKong(json.getString("fav_counts"))?"null":getCount(json.getString("fav_counts"));
			    		 //踩量
			    		 String step_counts=isKong(json.getString("step_counts"))?"null":getCount(json.getString("step_counts"));
			    		 //简介
			    		 String introduce=isKong(json.getString("introduce"))?"null":getCommon(json.getString("introduce")).replace("\n", ",").replace("\t", "").replace(",", "");
			    		 //编剧
			    		 String screenwriter=isKong(json.getString("screenwriter"))?"null":getCommon(json.getString("screenwriter"));
			    		 //语言
			    		 String language=isKong(json.getString("language"))?"null":getDouban_language(json.getString("language")); 
			    		 output=new Text(site_name+TAB+tv_name+TAB+tv_counts+TAB+update_counts+TAB+publish_time+TAB+firstplay_time+TAB+score_level+TAB+score_counts+TAB+
			    				 primary_stars+TAB+director+TAB+style_classify+TAB+area+TAB+play_counts+TAB+comment_counts+TAB+fav_counts+TAB+step_counts+TAB+introduce+TAB+screenwriter+TAB+language);
			    		 if(output.toString().split(TAB)[0].equals("douban_tv")){
				    		   mos.write("douban", output, new Text(), "Douban/");
			    		 }
			    	 }
			    	 
			    	 
			    	 
			   }
		   }
		   
		   
	}
		
	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(TV.class);
		job.setJobName("TV");
		job.setNumReduceTasks(1);
		job.setMapperClass(TvMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "youku", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "douban", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "souhu", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "tudou", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "letv", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "mangguo", TextOutputFormat.class,
                Text.class, NullWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}
	
	
	
	
}
