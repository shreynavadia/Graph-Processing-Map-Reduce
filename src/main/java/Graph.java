import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent = new Vector<>();// the vertex neighbors
    public int len;
    /* ... */

    public Vertex() {}

    public Vertex(short tag, long group, long vID, int len,Vector<Long> adjacent) {
        this.tag = tag;
        this.group = group;
        VID = vID;
        this.adjacent = adjacent;
        this.len=len;
    }

    public Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        out.writeInt(len);
        writeVector(adjacent,out);

    }
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        len = in.readInt();
        adjacent = readVector(in,tag,len);
    }

    public Vector<Long> readVector(DataInput in,Short tag,int len) throws IOException,EOFException {
        Vector<Long> adj=null;
        if(tag==0) {
            adj = new Vector<>();
            for(int i=0;i<len;i++)
            {
                adj.add(in.readLong());
            }
            return adj;
        }
        return adj;

    }

    public void writeVector(Vector<Long> adjacent,DataOutput out)
            throws IOException {
        if(adjacent!=null) {
            for(int i=0;i<adjacent.size();i++){
                out.writeLong(adjacent.get(i));
            }
        }
    }

    @Override
    public String toString() {
        return tag + " "+group+ " "+ VID + " "+ adjacent;
    }

}

public class Graph {
    public static class GraphMapper extends Mapper<Object,Text,
            LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            /* write your mapper code */
            long vid;
            Vector<Long> adj = new Vector<>();
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            vid = s.nextLong();
            while(s.hasNextLong()) {
                adj.add(s.nextLong());
            }
            Vertex v = new Vertex((short)0,vid,vid, adj.size(),adj);
            context.write(new LongWritable(vid),v);
            s.close();

        }
    }

    public static class ComputeMapper extends Mapper<Object,Text,
            LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            
            short tag;
            long vid;
            long Vertexid;
            long group;
            long element;
            String str;
            String temp;
            Vector<Long> adj = new Vector<>();
            Scanner s = new Scanner(value.toString());
            Vertexid = s.nextLong();
            tag = s.nextShort();
            group = s.nextLong();
            vid = s.nextLong();
            str = s.nextLine();
            str = str.replace("[","");
            str = str.replace("]","");
            str = str.replaceAll("\\s", "");
            String[] ar = str.split(",");
            for(int i=0;i<ar.length;i++) {
                element = Long.parseLong(ar[i]);
                adj.add(element);
            }
            Vertex v = new Vertex((short)0,group,vid, adj.size(),adj);
            context.write(new LongWritable(vid),v);
            for(Long n:adj) {
                Vertex t = new Vertex((short)1,group);
                context.write(new LongWritable(n),t);
            }
            s.close();
        }
    }

    public static class ComputeReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                throws IOException, InterruptedException {
            Long m = (long)100000;
            Vector<Long> adj = new Vector<>();
            for(Vertex v: values)
            {
                if(v.tag==0) {
                    adj=v.adjacent;
                }
                if(m>v.group)
                    m=v.group;

            }
            context.write(new LongWritable(m),new Vertex((short)0,m,key.get(),adj.size(),adj));

        }
    }

    public static class GroupMapper extends Mapper<Object,Text,
            LongWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            
            long group;
            Scanner s = new Scanner(value.toString());
            group  = s.nextLong();
            context.write(new LongWritable(group),new IntWritable(1));
            s.close();
        }
    }

    public static class GroupReducer extends Reducer<LongWritable,IntWritable,LongWritable,LongWritable> {
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            Long count=(long)0;
            for(IntWritable v: values)
            {
                count=count+v.get();
            }
            context.write(key,new LongWritable(count));

        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        
        job.setJarByClass(Graph.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapperClass(GraphMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
            job = Job.getInstance();
            job.setJobName("MyJob1");
            job.setJarByClass(Graph.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setMapperClass(ComputeMapper.class);
            job.setReducerClass(ComputeReducer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/f"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));
            job.waitForCompletion(true);
        }

        job = Job.getInstance();
        job.setJobName("MyJob2");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

    }
}