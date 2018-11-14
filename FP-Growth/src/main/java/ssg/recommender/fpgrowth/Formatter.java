package ssg.recommender.fpgrowth;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;


public class Formatter {
	public static Map<String, Long> readFrequency(Configuration configuration, String fileName) throws Exception {
		FileSystem fs = FileSystem.get(configuration);
		Reader frequencyReader = new SequenceFile.Reader(fs, 
				new Path(fileName), configuration);
		
		Map<String, Long> frequency = new HashMap<String, Long>();
		Text key = new Text();
		LongWritable value = new LongWritable();
		while(frequencyReader.next(key, value)) {
			frequency.put(key.toString(), value.get());
		}
		frequencyReader.close();
		return frequency;
	}
	
	public static Map<Integer, String> readMapping(String fileName) throws Exception {
		Map<Integer, String> itemById = new HashMap<Integer, String>();
		BufferedReader csvReader = new BufferedReader(new FileReader(fileName));
		while(true) {
			String line = csvReader.readLine();
			if (line == null) {
				break;
			}
			
			String[] tokens = line.split(",", 2);
			itemById.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		csvReader.close();
		return itemById;
	}
	
	public static void readFrequentPatterns(
			Configuration configuration,
			String fileName,
			int transactionCount,
			Map<String, Long> frequency,
			//Map<Integer, String> itemById,
			double minSupport, double minConfidence) throws Exception {
		FileSystem fs = FileSystem.get(configuration);

		Reader frequentPatternsReader = new SequenceFile.Reader(fs, 
				new Path(fileName), configuration);
		Text key = new Text();
		TopKStringPatterns value = new TopKStringPatterns();
		
		BufferedWriter bf = new BufferedWriter(new FileWriter("final.log",false));
		DecimalFormat df = new DecimalFormat("#0.0000000");
		
		int total = 0;
		while(frequentPatternsReader.next(key, value)) {
			
			long firstFrequencyItem = -1;
			String firstItemId = null;
			List<Pair<List<String>, Long>> patterns = value.getPatterns();
			int i = 0;
			for(Pair<List<String>, Long> pair: patterns) {
				List<String> itemList = pair.getFirst();
				Long occurrence = pair.getSecond();
				if (i == 0) {
					firstFrequencyItem = occurrence;
					firstItemId = itemList.get(0);
				} else {
					try{
						double support = (double)occurrence / transactionCount;
						double confidence = (double)occurrence / firstFrequencyItem;
						if (support > minSupport
								&& confidence > minConfidence) {
							List<String> listWithoutFirstItem = new ArrayList<String>();
							for(String itemId: itemList) {
								if (!itemId.equals(firstItemId)) {
									//listWithoutFirstItem.add(itemById.get(Integer.parseInt(itemId)));
									listWithoutFirstItem.add(itemId);
								}
							}
							//String firstItem = itemById.get(Integer.parseInt(firstItemId));
							String firstItem = firstItemId;
							listWithoutFirstItem.remove(firstItemId);
							System.out.printf(
								"%s => %s: supp=%.8f, conf=%.3f",
								firstItem,
								listWithoutFirstItem,
								support,
								confidence);

							
								String otherItemId = "";
								for(String itemId: itemList) {
									if (!itemId.equals(firstItemId)) {
										//otherItemId = Integer.parseInt(itemId);
										otherItemId = itemId;
										break;
									}
								}
								long otherItemOccurrence = frequency.get(otherItemId);
								
								

								double lift = ((double)occurrence * transactionCount) / (firstFrequencyItem * otherItemOccurrence);
								double conviction = (1.0 - (double)otherItemOccurrence / transactionCount) / (1.0 - confidence);
								System.out.printf(
									", lift=%.3f, conviction=%.3f",
									lift, conviction);
							
							System.out.printf("\n");
							total++;
							
							
//							FOR DEV							
//							Association result = connector.getAssociation(firstItem, listWithoutFirstItem.toString());
//							Association association = new Association();
//							if (result.getAntecedent() == null){
//								association.setAntecedent(firstItem);
//								association.setConsequent(listWithoutFirstItem.toString());
//								association.setSupport(support);
//								association.setConfidence(confidence);
//								association.setConviction(conviction);
//								association.setLift(lift);
//								
//							}else{
//								association.setAntecedent(firstItem);
//								association.setConsequent(listWithoutFirstItem.toString());
//								association.setSupport(support);
//								association.setConfidence(confidence);
//								association.setConviction(conviction);
//								association.setLift(lift + association.getLift());
//							}
//							
//							connector.insertAssociation(association);							
							
							for(String item : listWithoutFirstItem){
								StringBuilder sb = new StringBuilder();
								sb.append(firstItem).append("|").append(item).append("|").append(df.format(support)).append("|").append(df.format(confidence)).append("|").append(df.format(lift));
								bf.write(sb.toString());
								bf.newLine();
							}
						}
					}catch(Exception e){
						System.out.println(e);
						continue;
					}					
				}
				i++;
			}
		}
		frequentPatternsReader.close();
		System.out.println("Total Count = " + total);
		bf.close();
	}
	
	public static void main(String args[]) throws Exception {
		if (args.length != 5) {
			System.err.println("Arguments: [transaction count] [mapping.csv path] [fList path] "
					+ "[frequentPatterns path] [minSupport] [minConfidence]");
			return;
		}

		int transactionCount = Integer.parseInt(args[0]);
		String frequencyFilename = args[1];
		String frequentPatternsFilename = args[2];
		double minSupport = Double.parseDouble(args[3]);
		double minConfidence = Double.parseDouble(args[4]);		

		Configuration configuration = new Configuration();		
		Map<String, Long> frequency = readFrequency(configuration, frequencyFilename);
		readFrequentPatterns(configuration, frequentPatternsFilename, 
				transactionCount, frequency, minSupport, minConfidence);
		
	}
}