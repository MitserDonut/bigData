package work;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Mapreduce_WC {
	public static class Mymapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		//map������������ÿһ�����ݣ�������������key��value��context
		//context ���������࣬�𵽳������µ����á����������������Ϣ���Լ�д��д���Ĺ��� 
		@Override
		protected void map(LongWritable key, Text value,				
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//��text���͵��������ݣ�ת��string����
			 String line = value.toString();
			//�����ı����ݵķָ��������з�Ϊһ������
			 String[] words = line.split(",");
			//�������飬��ÿ������д��
			for (String word : words) {
				context.write(new Text(word), new LongWritable(1l));
			}
		}
	}
	
	//��ɧ�ķָ���------����reduce�׶�---��ͬ�ķ���ͨ��shuffle�Ὣ������ȡ��reduce
	//ÿһ��reduce�������ͬ��key��һ������
	//ָ��reduce��key ��value�������������
	public static class Myreducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	//���Ǹ����reduce����
	//ÿһ��reduce����������ͬkey��һ������
		@Override
		protected void reduce(Text key2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
		long sum =0l;
		//�����������е�value��Ҳ������ͬkey��һ��value
		for (LongWritable longvalue : v2s) {
			//��ÿ��value���
			sum=sum+longvalue.get();
		}
			//��������������д���ļ�������			
			context.write(key2, new LongWritable(sum));
			
		}
		
		
	}
	
//��������������ڣ���װ����mapreduce����
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//��ȡĬ��������Ϣ��������Զ����޸�һЩ����������ʹ�� conf.set()�������ò���
		Configuration conf = new Configuration();
		//����job����
		Job job = Job.getInstance(conf, Mapreduce_WC.class.getSimpleName());
		//ָ���������jar��
		job.setJarByClass(Mapreduce_WC.class);
		//ָ����ȡ���ݵ�·����Ҳ����������Դ�������ָhdfs�ϵ�·��,args[0]��������ĵ�һ������
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//ָ��map��
		job.setMapperClass(Mymapper.class);
		//ָ��map�м���������key��value�����л�����
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(LongWritable.class);
		//ָ��reduce��		
		job.setReducerClass(Myreducer.class);
		//ָ�����key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//ָ�����·��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//�ύ���񣬵ȴ����
		job.waitForCompletion(true);
		
	}
	
	

}
