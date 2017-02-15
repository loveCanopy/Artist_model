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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.alibaba.fastjson.JSONObject;
public class Xiami_album {

	
	public static String TAB="\t";
	//去除["()"]
	public static String getCount(String value){
		if(value!=null){
			return value.replace(",", "").replace("\"", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "");
		}else{
			return null;
		}
		
	}
		
		//去除[""] \n \r \\n \\r // 空格/空格   去空格
		public static String getCommon(String value){
			
			if(value!=null){
				return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "").trim();
			}else{
				return null;
			}
			
		}
		//album_counts ["共41张专辑"] ==  41
		public static String getAlbum_counts(String value){
				if(value!=null){
				Pattern p=Pattern.compile("\\[\"共(.*)张专辑\"\\]");
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
		//artist_name  ["莫文蔚的专辑"]  == 莫文蔚
		public static String getArtist_name(String value){
			if(value!=null){
			Pattern p=Pattern.compile("\\[\"(.*)的专辑\"\\]");
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
		
		
		//判断数据是否为空
		public static boolean isKong(String value){
			if(value==null||value.matches("\\[\"\"\\]")){
				return true;
			}else{
				return false;
			}
			
		}
		
	public static class XiamiAlbumMapper extends Mapper<LongWritable, Text, Text, Text>{
		   @Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			   if(value.toString().matches("\\{.*\\}")){
				   JSONObject json=JSONObject.parseObject(value.toString());
				   String site_name=isKong(json.getString("site_name"))?"null":getCommon(json.getString("site_name"));
			       if(site_name.matches(".*xiami_album.*")){
			    	   String album_name=isKong(json.getString("album_name"))?"null":getCommon(json.getString("album_name"));
			    	   //艺人名
			    	   String artist_name=isKong(json.getString("artist_name"))?"null":getArtist_name(json.getString("artist_name"));
			    	   //专辑总数
			    	   String album_counts=isKong(json.getString("album_counts"))?"null":getAlbum_counts(json.getString("album_counts"));
			    	   //专辑分享数
			    	   String share_counts=isKong(json.getString("share_counts"))?"null":getCount(json.getString("share_counts"));
			    	   //专辑喜爱数
			    	   String fav_counts=isKong(json.getString("fav_counts"))?"null":getCommon(json.getString("fav_counts"));
			    	   //评分
			    	   String score_level=isKong(json.getString("score_level"))?"null":getCommon(json.getString("score_level"));
		    	       //评价人数
			    	   String score_counts=isKong(json.getString("score_counts"))?"null":getCommon(json.getString("score_counts"));
			    	   //专辑收藏数
			    	   String collect_counts=isKong(json.getString("collect_counts"))?"null":getCommon(json.getString("collect_counts"));
			    	   
			    	   context.write(new Text(site_name), new Text(album_name+TAB+artist_name+TAB+album_counts+TAB+share_counts+TAB+fav_counts+TAB+score_level+TAB+
			    			   score_counts+TAB+collect_counts));
			       }
			   
			   }
			   
			   
		   }
	
	
	}   
	
	

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Xiami_album.class);
		job.setJobName("Xiami_album");
		job.setNumReduceTasks(0);
		job.setMapperClass(XiamiAlbumMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
		String out = "/user/work/ArtistModel/"+time+"/Xiami_album";
		Path path = new Path(out);
		hdfs.delete(path, true);
		Xiami_album.runLoadMapReducue(conf, args[0], new Path(out));
	}

}
