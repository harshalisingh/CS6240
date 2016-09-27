
/**
 * @author Harshali Singh
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


public class Prediction extends Configured implements Tool {

	//CONSTANTS
	private static final String OUTPUT_PATH = "intermediate_output";
	final static int FOUR = 4;
	static String SEPARATOR = "\t";
	final static SimpleDateFormat form = new SimpleDateFormat("yyyy-MM-dd");


	public static class PredictionMapper extends Mapper<Object, Text, Text, FlightWritable> {

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
					int crsDepartureTime = flight.getCrsDepartureTime();
					int crsArrivalTime = flight.getCrsArrivalTime();
					int delay = flight.getArrivalDelayMinutes() > 0 ? 1:0;
					Date date = convertToDate(flight.getFlightDate());
					int daysTillNearestHoliday = FlightUtils.closerDate(date, FlightUtils.getHolidays(date));
					//boolean holiday = (daysTillNearestHoliday < 3)? true:false;

					Text mapperOutKey = createKey(String.valueOf(month));
					FlightWritable flightWritable = new FlightWritable(year, month, dayOfMonth, dayOfWeek, carrier, flightNum, flightDate, 
							originId, destId, crsDepartureTime, crsArrivalTime, delay, daysTillNearestHoliday);

					context.write(mapperOutKey, flightWritable); 
				} catch (ParseException | NumberFormatException | ArrayIndexOutOfBoundsException e) {}



			}

		}

		/** Method to Create Mapper output Key
		 *  @param   String    carrier code, year, airport code
		 *  @return  String    Mapper output Key
		 */
		private Text createKey(String month) {
			Text returnKey = null;
			if (!month.isEmpty()) {
				returnKey = new Text(month);
			}
			return returnKey;
		}


		/**
		 * @param d
		 * Method takes string date and parses it to yyyy-MM-dd format
		 * */
		private static Date convertToDate(String d) throws ParseException {
			Date date = null;
			if(!d.isEmpty()){

				date = form.parse(d);
			}
			return date;
		}

	}

	public static class FlightTestMapper extends Mapper<Object, Text, Text, FlightWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//Split the record
			String line = value.toString();
			String newLine = line.replaceAll("\"", "");
			String formattedLine = newLine.replaceAll(", ", ":");
			String[] row = formattedLine.split(",");


			//Check if the flight passed sanity test.
			if (FlightUtils.sanityTest(row, true)) {

				//System.out.println("*****************************Passed");
				Flight flight = FlightParser.getFlightData(row, true);

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
					int crsDepartureTime = flight.getCrsDepartureTime();
					int crsArrivalTime = flight.getCrsArrivalTime();
					int delay = flight.getArrivalDelayMinutes() > 0 ? 1:0;
					Date date = convertToDate(flight.getFlightDate());
					int daysTillNearestHoliday = FlightUtils.closerDate(date, FlightUtils.getHolidays(date));

					Text mapperOutKey = createKey(String.valueOf(month));
					FlightWritable flightWritable = new FlightWritable(year, month, dayOfMonth, dayOfWeek, carrier, flightNum, flightDate, 
							originId, destId, crsDepartureTime, crsArrivalTime, delay, daysTillNearestHoliday);

					context.write(mapperOutKey, flightWritable); 
				} catch (ParseException | NumberFormatException | ArrayIndexOutOfBoundsException e) {}



			}

		}

		/** Method to Create Mapper output Key
		 *  @param   String    carrier code, year, airport code
		 *  @return  String    Mapper output Key
		 */
		private Text createKey(String month) {
			Text returnKey = null;
			if (!month.isEmpty()) {
				returnKey = new Text(month);
			}
			return returnKey;
		}


		/**
		 * @param d
		 * Method takes string date and parses it to yyyy-MM-dd format
		 * */
		private static Date convertToDate(String d) throws ParseException {
			Date date = null;
			if(!d.isEmpty()){

				date = form.parse(d);
			}
			return date;
		}

	}


	public static class PredictionReducer extends Reducer<Text, FlightWritable, Text, Text>{

		public void reduce(Text key, Iterable<FlightWritable> flights, Context context) throws IOException, InterruptedException{

			ArrayList<Attribute> fvWekaAttributes = getAttributes();

			Instances isTrainingSet = createInstance(fvWekaAttributes);

			for(FlightWritable flight : flights){

				Instance iFlight = new DenseInstance(9);

				iFlight.setValue((Attribute)fvWekaAttributes.get(0), flight.getDayOfMonth()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(1), flight.getDayOfWeek()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(2), flight.getCarrier().hashCode()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(3), flight.getOriginId()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(4), flight.getDestId()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(5), flight.getCrsDepartureTime()); 
				iFlight.setValue((Attribute)fvWekaAttributes.get(6), flight.getCrsArrivalTime()); 

				iFlight.setValue((Attribute)fvWekaAttributes.get(7), flight.getDaysTillNearestHoliday()); 


				iFlight.setValue((Attribute)fvWekaAttributes.get(8), flight.getDelay()); 

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

		private Instances createInstance(ArrayList<Attribute> attributes) {	

			Instances instance = new Instances("Model", attributes, 0);

			instance.setClassIndex(8);

			return instance;
		}
	}

	public static class FlightTestReducer extends Reducer<Text, FlightWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<FlightWritable> flights, Context context) throws IOException {


			ArrayList<Attribute> fvWekaAttributes = getAttributes();
			Instances isTestingSet = createInstance(fvWekaAttributes);

			try{
				Configuration conf = new Configuration();
				String path = "models/";
				URI uri = new URI(path);
				FileSystem fs = FileSystem.get(uri, conf);
				Path modelPath = new Path(path + key + ".model");
				FSDataInputStream inStream = fs.open(modelPath);
				ObjectInputStream ois = new ObjectInputStream(inStream);

				//Classifier deserialization
				Classifier classifier = (Classifier) ois.readObject();

				for(FlightWritable flight : flights){

					Instance iFlight = new DenseInstance(9);

					iFlight.setValue((Attribute)fvWekaAttributes.get(0), flight.getDayOfMonth()); 
					iFlight.setValue((Attribute)fvWekaAttributes.get(1), flight.getDayOfWeek()); 
					iFlight.setValue((Attribute)fvWekaAttributes.get(2), flight.getCarrier().hashCode()); 
					iFlight.setValue((Attribute)fvWekaAttributes.get(3), flight.getOriginId()); 
					iFlight.setValue((Attribute)fvWekaAttributes.get(4), flight.getDestId()); 
					iFlight.setValue((Attribute)fvWekaAttributes.get(5), flight.getCrsDepartureTime()); 
					iFlight.setValue((Attribute)fvWekaAttributes.get(6), flight.getCrsArrivalTime()); 

					iFlight.setValue((Attribute)fvWekaAttributes.get(7), flight.getDaysTillNearestHoliday()); 


					iFlight.setValue((Attribute)fvWekaAttributes.get(8), flight.getDelay()); 
					
					iFlight.setDataset(isTestingSet);

					// Get the prediction probability distribution.
					double[] predictionDistribution = classifier.distributionForInstance(iFlight); 

					String prediction = null;

					if(predictionDistribution[0] > predictionDistribution[1]){

						prediction = "TRUE";
						
					} else prediction = "FALSE";

					int flightNum = flight.getFlightNum();
					String flightDate = flight.getFlightDate();
					int crsDepTime = flight.getCrsDepartureTime();

					Text finalKey = new Text(flightNum+"_"+flightDate+"_"+crsDepTime);
					Text finalValue = new Text(prediction.toUpperCase());

					context.write(finalKey, finalValue);

				}

			} catch (Exception ex){

				System.out.println("Exception in testing the classifier."+ ex);

			}



		}

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

		Job job1 = new Job(configuration, "Prediction");
		job1.setJobName("PredictionJob1");
		job1.setJarByClass(Prediction.class);

		FileInputFormat.addInputPath(job1, new Path(args[0] + "/a6history"));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH));

		job1.setMapperClass(PredictionMapper.class);
		job1.setReducerClass(PredictionReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FlightWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.submit();

		job1.waitForCompletion(true);
		
		/* Job2
		 * 
		 */

		Job job2 = new Job(configuration, "Prediction");
		job2.setJobName("PredictionJob2");
		job2.setJarByClass(Prediction.class);

		FileInputFormat.addInputPath(job2, new Path(args[0] + "/a6test"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setMapperClass(FlightTestMapper.class);
		job2.setReducerClass(FlightTestReducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(FlightWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.submit();

		return job2.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new Prediction(), args));
	}

}
