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

public class Music {

	public static final String TAB = "\001";
	    //判断数据是否为空
			public static boolean isKong(String value){
				if(value==null||value.matches("\\[\"\"\\]")){
					return true;
				}else{
					return false;
				}
				
			}

		//去除[""] \n \r \\n \\r // 空格/空格   去空格
				public static String getCommon(String value){
					
					if(value!=null){
						return value.replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\r", "").replace("\\n", "").replace("\\r", "").replace("//", "").replace(" / ", "").replace(" ", "").trim();
					}else{
						return null;
					}
					
				}
			public static String getCount(String value){
				if(value!=null){
					return value.replace(",", "").replace("\"", "").replace("[", "").replace("]", "").replace("\n", "").replace("\\n", "").trim();
				}else{
					return null;
				}
				
			}
			
			//QQmusic粉丝关注
			public static String getQQmusicFav_counts(String value){
				if(value!=null){
				Pattern p=Pattern.compile("\\[\"关注 (.*)\"\\]");
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
			//虾米music 艺人名
			public static boolean isKong_xiamimusic(String value){
				if(value==null||value.matches("\\[\"\"\\]")||value.trim().matches("\\[\"\",\"\"\\]")){
					return true;
				}else{
					return false;
				}
				
			}
	
	public static class MusicMapper extends Mapper<LongWritable, Text, Text, Text>{
		  		   
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
		    	   
		    	   if(site_name.matches(".*qq_music.*")){
		    	   //艺人名
		    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
		    	   //更新时间
		    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
		    	   //粉丝量、喜欢量
		    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getQQmusicFav_counts(json.getString("fans_counts"));
		    	   //总专辑量
		    	   String album_counts=isKong(json.getString("album_counts"))?"null":getCount(json.getString("album_counts"));
		    	   //总单曲量
		    	   String song_counts=isKong(json.getString("song_counts"))?"null":getCount(json.getString("song_counts"));
		    	   //MV量
		    	   String mv_counts=isKong(json.getString("mv_counts"))?"null":getCount(json.getString("mv_counts"));
		    	   //动态量
		    	   String dynamic_counts=isKong(json.getString("dynamic_counts"))?"null":getCount(json.getString("dynamic_counts"));
		    	   //评论量
		    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
		    	   //艺人总播放量
		    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
		    	   //流派
		    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
		    	   //歌曲名
		    	   String song_name=isKong(json.getString("song_name"))?"null":getCommon(json.getString("song_name"));
		    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+fans_counts+TAB+album_counts+TAB+song_counts+TAB+mv_counts
		    			   +TAB+dynamic_counts+TAB+comment_counts+TAB+play_counts+TAB+style_classify+TAB+song_name);
		    	   mos.write("qqmusic", output, new Text(), "QQmusic/");
		    	   
		    	   }else if(site_name.matches(".*douban.*")||site_name.matches(".*xiami_music.*")){
				 //艺人名
		    	   String artist_name=isKong(json.getString("artist_name"))?"null":getCommon(json.getString("artist_name"));
		    	   //更新时间
		    	   String update_time=isKong(json.getString("update_time"))?"null":getCommon(json.getString("update_time"));
		    	   //粉丝量、喜欢量
		    	   String fans_counts=isKong(json.getString("fans_counts"))?"null":getCount(json.getString("fans_counts"));
		    	   //总专辑量
		    	   String album_counts=isKong(json.getString("album_counts"))?"null":getCount(json.getString("album_counts"));
		    	   //总单曲量
		    	   String song_counts=isKong(json.getString("song_counts"))?"null":getCount(json.getString("song_counts"));
		    	   //MV量
		    	   String mv_counts=isKong(json.getString("mv_counts"))?"null":getCount(json.getString("mv_counts"));
		    	   //动态量
		    	   String dynamic_counts=isKong(json.getString("dynamic_counts"))?"null":getCount(json.getString("dynamic_counts"));
		    	   //评论量
		    	   String comment_counts=isKong(json.getString("comment_counts"))?"null":getCount(json.getString("comment_counts"));
		    	   //艺人总播放量
		    	   String play_counts=isKong(json.getString("play_counts"))?"null":getCount(json.getString("play_counts"));
		    	   //流派
		    	   String style_classify=isKong(json.getString("style_classify"))?"null":getCommon(json.getString("style_classify"));
		    	   //歌曲名
		    	   String song_name=isKong(json.getString("song_name"))?"null":getCommon(json.getString("song_name")).replace(",", "");
		    	   output=new Text(site_name+TAB+artist_name+TAB+update_time+TAB+fans_counts+TAB+album_counts+TAB+song_counts+TAB+mv_counts
		    			   +TAB+dynamic_counts+TAB+comment_counts+TAB+play_counts+TAB+style_classify+TAB+song_name);	   
		    	   if(output.toString().split(TAB)[0].equals("douban")){
			    		mos.write("douban", output, new Text(), "Douban/");
			    	} else if(output.toString().split(TAB)[0].equals("xiami_music")){
			    		mos.write("xiamimusic", output, new Text(), "Xiamimusic/");
			    	}			 
		    	   
			   }  	
			  }
		}
	}
	
	
	  

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Music.class);
		job.setJobName("Music");
		job.setNumReduceTasks(1);
		job.setMapperClass(MusicMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job, "qqmusic", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "douban", TextOutputFormat.class,
                Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job, "xiamimusic", TextOutputFormat.class,
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
		String out = "/user/work/ArtistModel/"+time+"/Music";
		Path path = new Path(out);
		hdfs.delete(path, true);
		Music.runLoadMapReducue(conf, args[0], new Path(out));
	}

}

