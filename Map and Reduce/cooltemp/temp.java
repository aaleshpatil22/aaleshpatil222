import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;


public class temp {
	
    public static class tempMapper extends Mapper<Object, Text, Text, FloatWritable>{
        public void map(Object key, Text value, Context context)  throws IOException, InterruptedException{
              
             String line[] = value.toString().split("\\s+"); 
             float maxTemp = Float.parseFloat(line[6]);
             String year = line[1].substring(0,4);
             if (maxTemp>-60.0f && maxTemp<60.0f){
                 context.write(new Text(year), new FloatWritable(maxTemp));
             
             }   
        }
    }
     
    public static class tempReducer extends Reducer< Text, FloatWritable, Text, FloatWritable >{
         String year = null;
         float globalTemp = 0.0f;                                                          
         public void reduce(Text key,
                            Iterable<FloatWritable> values,
                            Context context) throws IOException, InterruptedException{
             float min = 100;
             for(FloatWritable temp : values){
                 if(temp.get()<min)
                     min = temp.get();
             }
      
          if (min<globalTemp){
               globalTemp = min;
               year = key.toString();
          
          }
             //context.write(new Text(year), new FloatWritable(max));  
                                  
         }
         
         public void cleanup(Context context)throws IOException, InterruptedException{
         
               context.write(new Text(year), new FloatWritable(globalTemp));
               
         }
    }
   
    public static void main(String args[]) throws Exception{
    
    Configuration conf = new Configuration();
    
    Job job = new Job(conf,"Hottest year");
    
    job.setOutputKeyClass(Text.class);
    
    job.setOutputValueClass(FloatWritable.class);
    
    job.setOutputFormatClass(TextOutputFormat.class);
   
    job.setInputFormatClass(TextInputFormat.class);
    
    job.setMapperClass(tempMapper.class);
    
    job.setReducerClass(tempReducer.class);
    
    //job.setNumReduceTasks(0);
    
    FileInputFormat.addInputPath(job,new Path(args[0]));
    
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    
    job.waitForCompletion(true);
   
   }
      
}
