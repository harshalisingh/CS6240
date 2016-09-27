/**
 * @author Harshali Singh, Vishal Mehta
 *
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import weka.core.Instance;
import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.DenseInstance;


public class Routing extends Configured implements Tool {

	//CONSTANTS
	private static final String OUTPUT_PATH = "intermediate_output";
	final static int FOUR = 4;
	static String SEPARATOR = "\t";

	public static class RoutingMapper extends Mapper<Object, Text, Text, FlightWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//Split the record
			String line = value.toString();
			String newLine = line.replaceAll("\"", "");
			String formattedLine = newLine.replaceAll(", ", ":");
			String[] row = formattedLine.split(",");

			//Check if the flight passed sanity test.
			if (FlightUtils.sanityTest(row)) {

				Flight flight = FlightParser.getFlightData(row);

				try{

					int year = flight.getYear();
					int month = flight.getMonth();
					int dayOfMonth = flight.getDayOfMonth();
					int dayOfWeek = flight.getDayOfWeek();
					String carrier = flight.getCarrier();
					int flightNum = flight.getFlightNum();
					String flightDate = flight.getFlightDate();

					int originId = flight.getOriginAirportId();
					int destId = flight.getDestAirportId();

					String origin = flight.getOrigin();
					String dest = flight.getDest();

					int crsDepartureTime = flight.getCrsDepartureTime();
					int crsArrivalTime = flight.getCrsArrivalTime();
					int crsElapsedTime = flight.getCrsElapsedTime();

					double arrDelay = flight.getArrivalDelay();
					double depDelay = flight.getDepartureDelay();

					long ScheduleArrival = FlightUtils.getScheduleTimeInMs(flightDate, crsArrivalTime, arrDelay);
					long ScheduleDeparture = FlightUtils.getScheduleTimeInMs(flightDate, crsDepartureTime, depDelay);

					int delay = flight.getArrivalDelayMinutes() > 0 ? 1:0;
					Date date = FlightUtils.convertToDate(flight.getFlightDate());
					int daysTillNearestHoliday = FlightUtils.closerDate(date, FlightUtils.getHolidays(date));

					Text mapperOutKey = createKey(String.valueOf(month));

					FlightWritable fWritable = new FlightWritable(year, month, dayOfMonth, dayOfWeek, carrier, flightNum, flightDate, 
							originId, destId, origin, dest, crsDepartureTime, crsArrivalTime, crsElapsedTime, ScheduleArrival,
							delay, daysTillNearestHoliday, true);

					context.write(mapperOutKey, fWritable); 
				} catch (ParseException | NumberFormatException | ArrayIndexOutOfBoundsException e) {

					System.out.println("Exception in Mapper: " + e);

				}



			}

		}

		/** Method to Create Mapper output Key
		 *  @param   String    month
		 *  @return  String    Mapper output Key
		 */
		private Text createKey(String month) {
			Text returnKey = null;
			if (!month.isEmpty()) {
				returnKey = new Text(month);
			}
			return returnKey;
		}





	}


	public static class RoutingReducer extends Reducer<Text, FlightWritable, Text, Text>{

		public void reduce(Text key, Iterable<FlightWritable> flights, Context context) throws IOException, InterruptedException{

			//Attributes
			ArrayList<Attribute> fvWekaAttributes = getAttributes();

			//Training Set
			Instances isTrainingSet = createInstance(fvWekaAttributes);

			for(FlightWritable flight : flights){

				Instance iFlight = new DenseInstance(9);

				iFlight.setValue((Attribute)fvWekaAttributes.get(0), flight.dayOfMonth); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(1), flight.dayOfWeek); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(2), flight.carrier.hashCode()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(3), flight.originId); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(4), flight.destId); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(5), flight.crsDepartureTime); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(6), flight.crsArrivalTime); 

				iFlight.setValue((Attribute)fvWekaAttributes.get(7), flight.daysTillNearestHoliday); 


				iFlight.setValue((Attribute)fvWekaAttributes.get(8), flight.delay); 

				isTrainingSet.add(iFlight);

			}

			// Create a naïve bayes classifier
			Classifier cModel = (Classifier)new NaiveBayes();
			try {

				cModel.buildClassifier(isTrainingSet);

				Configuration conf = new Configuration();
				String path = "models/";
				URI uri = new URI(path);
				FileSystem fs = FileSystem.get(uri, conf);
				Path modelPath = new Path(path + key + ".model");

				FSDataOutputStream stream = fs.create(modelPath);

				//storing the trained classifier to a file for future use
				weka.core.SerializationHelper.write(stream, cModel);


			} catch (Exception e) {

				e.printStackTrace();
			}

			//context.write(key, cModel);

		}

		/** Method to create Attribute List
		 *  @return  ArrayList<Attribute>    list of attributes
		 */

		private ArrayList<Attribute> getAttributes() {

			ArrayList<Attribute> attributes = new ArrayList<Attribute>();

			// Declare the class attribute along with its values
			ArrayList<String> clsDelay = new ArrayList<String>(2);
			clsDelay.add("true");
			clsDelay.add("false");			 
			Attribute delay = new Attribute("delay", clsDelay);

			attributes.add(new Attribute("dayOfMonth"));
			attributes.add(new Attribute("dayOfWeek"));
			attributes.add(new Attribute("carrier"));
			attributes.add(new Attribute("originId"));
			attributes.add(new Attribute("destId"));
			attributes.add(new Attribute("crsDepartureTime"));
			attributes.add(new Attribute("crsArrivalTime"));	
			attributes.add(new Attribute("daysTillNearestHoliday"));
			attributes.add(delay);

			return attributes;

		}


		/** Method to create Instances datset
		 *  @return  Instances   dataset of Instance
		 */

		private Instances createInstance(ArrayList<Attribute> attributes) {	

			Instances instance = new Instances("Model", attributes, 0);

			instance.setClassIndex(8);

			return instance;
		}
	}


	/**
	 *	Driver Program to run the jobs and set the input and output paths.
	 */
	public int run (String[] args) throws Exception {

		/* Job1
		 * 
		 */

		Configuration configuration = new Configuration();

		/*Job job1 = new Job(configuration, "Routing");
		job1.setJobName("RoutingJob1");
		job1.setJarByClass(Routing.class);

		FileInputFormat.addInputPath(job1, new Path(args[0] + "/a7history"));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

		job1.setMapperClass(RoutingMapper.class);
		job1.setReducerClass(RoutingReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FlightWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.submit();

		job1.waitForCompletion(true);*/

		/* Job2
		 * 
		 */

		/*	Job job2 = new Job(configuration, "Routing");
		job2.setJobName("RoutingJob2");
		job2.setJarByClass(Routing.class);

		FileInputFormat.addInputPath(job2, new Path(args[0] + "/a7test"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setMapperClass(Connection.ConnectionMapper.class);
		job2.setReducerClass(Connection.ConnectionReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(FlightWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.submit();

		return job2.waitForCompletion(true) ? 0 : 1;*/

		/* Job3
		 * 
		 */

		Job job3 = new Job(configuration, "Routing");
		job3.setJobName("RoutingJob3");
		job3.setJarByClass(Routing.class);

		FileSystem fs = FileSystem.getLocal(configuration);
		Path inputPath = fs.makeQualified(new Path("cons"));  // local path

		FileInputFormat.addInputPath(job3, new Path(inputPath+""));
		FileOutputFormat.setOutputPath(job3, new Path("optimal"));

		job3.setMapperClass(Request.RequestMapper.class);
		job3.setReducerClass(Request.RequestReducer.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.submit();

		return job3.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("usage: [input_history] [output]");
			System.exit(-1);
		}

		System.exit(ToolRunner.run(new Routing(), args));
	}

}
