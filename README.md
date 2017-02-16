# Artist_model日志清洗
## MV  清洗MV数据
## Media Media_sp 清洗综艺数据
## Movie 清洗电影数据
## Music 清洗音乐数据
## TV 清洗电视剧数据
## Weibo 清洗微博数据
## Xiami_album 清洗虾米专辑数据
## MR 多目录输出
### 放到Map中 或Reduce 中
//将结果输出到多个文件或多个文件夹  
public MultipleOutputs<Text,Text> mos;
//创建MultipleOutputs对象  
public void setup(Context context) throws IOException,InterruptedException {  
    mos = new MultipleOutputs<Text, Text>(context);  
 } 
 
 if(output.toString().split(TAB)[0].equals("acfun")){
				    		   mos.write("acfun", output, new Text(), "Acfun/");
				    	   }else if(output.toString().split(TAB)[0].equals("letv")){
				    		   mos.write("letv", output, new Text(), "Letv/");
				    	   }else if(output.toString().split(TAB)[0].equals("qq_mv")){
				    		   mos.write("qq", output, new Text(), "QQ/");
				    	   }else if(output.toString().split(TAB)[0].equals("xiami_mv")){
				    		   mos.write("xiami", output, new Text(), "Xiami/");
				    	   }
### 放到runLoadMapreduce中
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
### 设定产生的日期文件夹
Date dNow = new Date();  
Date dBefore = new Date();
Calendar calendar = Calendar.getInstance(); //得到日历
calendar.setTime(dNow);//把当前时间赋给日历
calendar.add(Calendar.DAY_OF_MONTH, -1);  //设置为前一天
dBefore = calendar.getTime();   //得到前一天的时间
SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //设置时间格式
String time = sdf.format(dBefore);    //格式化前一天
String out = "/user/work/ArtistModel/"+time+"/MV";
