/**
 * @author Vishal Mehta
 *
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Request {

	
	/**
	 * Mapper class.
	 */
	
public static class RequestMapper extends Mapper<Object, Text, Text, Text> {
	
	final static String REQUEST_FILE = "04req10k.csv.gz";
	
		Set<String> requestSet = new HashSet<String>();
		
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
		try{
			
			//Read the request file
			Configuration conf = new Configuration();
			String line = null;
			String path = "input/a7request/";
			URI uri = new URI(path);
			FileSystem fs = FileSystem.get(uri, conf);
			Path requestPath = new Path(path + REQUEST_FILE);
			FSDataInputStream inStream = fs.open(requestPath);
			
			GZIPInputStream gzip = new GZIPInputStream(inStream);
			Reader reader = new InputStreamReader(gzip);
			BufferedReader br = new BufferedReader(reader);
			
			//Create request Set
			while((line = br.readLine()) != null) {
				int length = line.length();
				String[] res = line.trim().split(",");
				
				String resKey = res[0]+","+res[1]+","+res[2]+","+res[3]+","+res[4];
				requestSet.add(resKey);
				
			}
			
			if (requestSet.contains(key.toString())){
				context.write(new Text(key.toString()), value);
			}
			
			
			br.close();
			reader.close();
			inStream.close();

		} catch(Exception ex) {
			System.out.println("Exception in mapper."+ ex);
		}

	}
}

	/**
	 * Reducer class.
	 */
	public static class RequestReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			List<String> optimal = new ArrayList<String>();
			
			for (Text value : values){
				
				//Remove the existing connection route and adding the route with lower duration time
				
				String[] newcons = value.toString().split(",");
			
				int newduration = Integer.parseInt(newcons[3]);
			
				if (!optimal.isEmpty()) {
				
					String old = optimal.get(0);
					
					String[] oldcons = old.toString().split(",");
				
					int oldduration = Integer.parseInt(oldcons[3]);
				
					if(newduration < oldduration){
						
						optimal.remove(0);
						optimal.add(value.toString());
					}
				
				}
							
			}
			
				context.write(key,new Text(optimal.get(0)));
		}
	}
}