
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * ClusterAnalyis -- The map reduce program that emits the avera	ge
 * ticket prices for each airline in the year 2015. 
 * 
 */
public class ClusterAnalysis extends Configured implements Tool{

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new ClusterAnalysis(), args));
	}

	/*
	 * Mapper program to read the entire data and filter out the flights which
	 * are not active in the year 2015 and passes the key value pair to the
	 * reduce program. Key is the carrier code and Value is the Avg. ticket
	 * price for that particular airline.
	 * 
	 */
	public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String carrierCode = new String();
			String avgTicketPrice = null;
			int year = 0;
			String month = null;
			boolean nullCheck = true;
			String replaceline = line.replace(", ", "?");
			String[] row = replaceline.split(",");
			// System.out.println(row);

			try {
				year = Integer.parseInt(row[0]);
			} catch (NumberFormatException ex) {
				nullCheck = false;
			}
			// System.out.println(row.length + "," + year + "," + isNull);

			if (row.length == 110 && year == 2015 && nullCheck) {

				carrierCode = row[6];
				avgTicketPrice = row[109];
				month = row[2];
				// System.out.println(carrierCode + "," + avgTicketPrice);

				Text outKey = null;
				Text outValue = null;

				outKey = createKey(carrierCode, month);
				outValue = createValue(avgTicketPrice);

				context.write(outKey, outValue);
			}

		}

		private Text createKey(String carrier, String month) {
			Text returnKey = null;
			if (!carrier.isEmpty() && !month.isEmpty()) {
				returnKey = new Text(carrier.trim() + "\t" + month.trim());
			}
			return returnKey;
		}

		private Text createValue(String avgPrice) {
			Text returnValue = null;
			if (!avgPrice.isEmpty()) {
				returnValue = new Text(avgPrice);
			}
			return returnValue;
		}
	}

	/*
	 * Reducer program to emit the consolidated avg. ticket prices for each
	 * airline in the year 2015. It takes the key value pair from the mapper.
	 * key is the carrier code and Value is the avg. ticket prices for the
	 * airline.
	 * 
	 */
	public static class FlightReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, IndexOutOfBoundsException {
			// System.out.println(values.toString());

			float sum = 0;
			int count = 0;
			for (Text value : values) {
				// System.out.println("Inside " +value.toString());
				sum = sum + Float.parseFloat(value.toString());
				count = count + 1;
			}

			float avg = (float) sum / count;
			// System.out.println(avg + "done");
			context.write(key, new Text(count + "\t" + String.valueOf(avg)));

		}
	}

	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length != 2) {
			System.err.println("Usage: ClusterAnalysis <input-path> <output-path>");
			return 1;
		}
		Job job = Job.getInstance(getConf(), "ClusterAnalysis");

		// job.getConfiguration().set("join.type", joinType);
		job.setNumReduceTasks(5);
		job.setJar("ClusterAnalysis.jar");
		job.setMapperClass(FlightMapper.class);
		job.setReducerClass(FlightReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
