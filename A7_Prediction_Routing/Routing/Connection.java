/**
 * @author Harshali Singh
 *
 */

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class Connection {

	//CONSTANTS
	final static long THIRTY_MINS_MS = 30 * 60000;
	final static long ONE_HRS_IN_MS = 60 * 60000;

	public static class ConnectionMapper extends Mapper<Object, Text, Text, FlightWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//Split the record
			String line = value.toString();
			String newLine = line.replaceAll("\"", "");
			String formattedLine = newLine.replaceAll(", ", ":");
			String[] row = formattedLine.split(",");

			//Check if the flight passed sanity test.
			if (FlightUtils.sanityTest(row, true)) {

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

					Text fOutKey = createKey(String.valueOf(year), String.valueOf(month), carrier, String.valueOf(dest));
					Text gOutKey = createKey(String.valueOf(year), String.valueOf(month), carrier, String.valueOf(origin));

					FlightWritable fWritable = new FlightWritable(year, month, dayOfMonth, dayOfWeek, carrier, flightNum, flightDate, 
							originId, destId, origin, dest, crsDepartureTime, crsArrivalTime, crsElapsedTime, ScheduleArrival,
							delay, daysTillNearestHoliday, true);

					FlightWritable gWritable = new FlightWritable(year, month, dayOfMonth, dayOfWeek, carrier, flightNum, flightDate, 
							originId, destId, origin, dest, crsDepartureTime, crsArrivalTime, crsElapsedTime, ScheduleDeparture,
							delay, daysTillNearestHoliday, false);

					context.write(fOutKey, fWritable); 
					context.write(gOutKey, gWritable); 

				} catch (Exception ex) {


					System.out.println("Exception in Mapper"+ ex);

				}



			}

		}

		/** Method to Create Mapper output Key
		 *  @param   String    carrier code, year, airport code
		 *  @return  String    Mapper output Key
		 */
		private Text createKey(String year, String month, String carrier, String airport) {
			Text returnKey = null;
			if (!carrier.isEmpty() && !year.isEmpty() && !airport.isEmpty() && !month.isEmpty()) {
				returnKey = new Text(year.trim() + "," + month.trim() + "," +carrier.trim() + "," + airport.trim());
			}
			return returnKey;
		}

	}

	public static class ConnectionReducer extends Reducer<Text, FlightWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<FlightWritable> flights, Context context) throws IOException {

			//Array List of arriving flight objects
			List<FlightWritable> arrList = new LinkedList<FlightWritable>();

			//Array List of departing flight objects
			List<FlightWritable> depList = new LinkedList<FlightWritable>();

			String newkey = key.toString();
			String[] splitkey = newkey.split(",");
			String month = splitkey[1];

			for (FlightWritable flight : flights) {

				FlightWritable fo = new FlightWritable(flight.year, flight.month, flight.dayOfMonth, flight.dayOfWeek, flight.carrier, flight.flightNum, flight.flightDate, 
						flight.originId, flight.destId, flight.origin, flight.dest, flight.crsDepartureTime, flight.crsArrivalTime, 
						flight.crsElapsedTime, flight.scheduledTime,
						flight.delay, flight.daysTillNearestHoliday, flight.isArrival);
				if (fo.isArrival)
					arrList.add(fo);
				else
					depList.add(fo);
			}

			//Sort flights based on scheduled times
			Collections.sort(arrList);
			Collections.sort(depList);

			ArrayList<Attribute> fvWekaAttributes = getAttributes();
			Instances isTestingSet = createInstance(fvWekaAttributes);
			try{

				Configuration conf = new Configuration();
				String path = "models/";
				URI uri = new URI(path);
				FileSystem fs = FileSystem.get(uri, conf);
				Path modelPath = new Path(path + month + ".model");
				FSDataInputStream inStream = fs.open(modelPath);
				ObjectInputStream ois = new ObjectInputStream(inStream);

				String mpath = "missed/";
				URI muri = new URI(mpath);
				FileSystem mfs = FileSystem.get(muri, conf);
				Path missedPath = new Path(path + "missed");
				FSDataOutputStream stream = fs.create(missedPath);

				//Classifier deserialization
				Classifier classifier = (Classifier) ois.readObject();

				for (FlightWritable f : arrList) {


					for (FlightWritable g : depList) {


						if ((g.scheduledTime - f.scheduledTime) < ONE_HRS_IN_MS) {
							if ((f.scheduledTime + THIRTY_MINS_MS) < g.scheduledTime
									&& g.scheduledTime < (f.scheduledTime + ONE_HRS_IN_MS)) {
								//connections

								Instance iFlight = new DenseInstance(9);

								iFlight.setValue((Attribute)fvWekaAttributes.get(0), f.dayOfMonth); 
								iFlight.setValue((Attribute)fvWekaAttributes.get(1), f.dayOfWeek); 
								iFlight.setValue((Attribute)fvWekaAttributes.get(2), f.carrier.hashCode()); 
								iFlight.setValue((Attribute)fvWekaAttributes.get(3), f.originId); 
								iFlight.setValue((Attribute)fvWekaAttributes.get(4), f.destId); 
								iFlight.setValue((Attribute)fvWekaAttributes.get(5), f.crsDepartureTime); 
								iFlight.setValue((Attribute)fvWekaAttributes.get(6), f.crsArrivalTime); 

								iFlight.setValue((Attribute)fvWekaAttributes.get(7), f.daysTillNearestHoliday); 


								iFlight.setValue((Attribute)fvWekaAttributes.get(8), f.delay); 

								iFlight.setDataset(isTestingSet);

								// Get the prediction probability distribution.
								double[] predictionDistribution = classifier.distributionForInstance(iFlight); 


								//System.out.println ("Distribution : [" + predictionDistribution[0] + " " + predictionDistribution[1] + "]");
								if(predictionDistribution[0] > predictionDistribution[1]){

									//bw.write(f.year + "," + f.month + "," + f.dayOfMonth + "," + f.origin + "," + g.dest + "," + f.flightNum + "," + g.flightNum);

									//missed connections
									stream.writeUTF(f.year + "," + f.month + "," + f.dayOfMonth + "," + f.origin + "," + g.dest + "," + f.flightNum + "," + g.flightNum);

									continue;

								} else {

									Text outKey = new Text (f.year + "," + f.month + "," + f.dayOfMonth + "," + f.origin + "," + g.dest);
									long duration = f.crsElapsedTime + g.crsElapsedTime + ((g.scheduledTime - f.scheduledTime)/60000);
									Text outVal = new Text(f.flightNum + "," + g.flightNum + "," + duration );

									//true connections
									context.write(outKey, outVal);

								}


							}

						} else {
							break;
						}
					}
				}


				stream.close();


			} catch (Exception ex){

				System.out.println("Exception in testing the classifier."+ ex);

			}


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


}
