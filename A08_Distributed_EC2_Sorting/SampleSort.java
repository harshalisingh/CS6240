//@Author: Harshali, Akanksha, Vishal, Saahil


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SampleSort {
	
	private int processors;

	public SampleSort(int numNodes) {
		this.processors = numNodes;
	}
	
	/**
	 * Phase 1 - Each process sorts its local share of the initial elements. 
	 * 
	 * @throws InterruptedException
	 */
	public List<FileData> sortLocalData(List<FileData> localData) throws InterruptedException{
		
		Collections.sort(localData);		
		
		return localData;
	}
	
	/**
	 * Phase 1.5 - Each process gets a regular sample of its locally sorted
	 * block.
	 * 
	 * @param sorters
	 * @return
	 */
	public List<FileData> sampleSections(List<FileData> sortedLocalData) {
		
		List<FileData> samples = new ArrayList<FileData>();
		SorterHelper sh = new SorterHelper(sortedLocalData);
		samples.addAll(sh.getSamples(processors));
		
		return samples;
	}
	
	/**
	 * Phase 2 - One process gathers and sorts the local regular samples. It
	 * selects p - 1 pivot values from the sorted list of regular samples. Each
	 * process partitions its sorted sublist into p disjoint pieces, using the
	 * pivot values as separators between the pieces.
	 * 
	 * @throws InterruptedException
	 */
	public List<FileData> getPivotsFromSamples(List<FileData> samples)
			throws InterruptedException {
		
		SorterHelper sh = new SorterHelper(samples);
		List<FileData> pivots = sh.getSamples(processors);
		return pivots.subList(pivots.size() - (processors - 1), pivots.size());
	}

}