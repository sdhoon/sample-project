import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

//10.203.5.76 test server
public class ItemRecommender {
	
	public static void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File(args[0]));		
		ItemSimilarity similarity = new LogLikelihoodSimilarity(model);
		GenericItemBasedRecommender itemRecommender = new GenericItemBasedRecommender(model, similarity);
		
		
		LongPrimitiveIterator iter = model.getItemIDs();
		BufferedWriter fw = new BufferedWriter(new FileWriter("itembased_result.log", false));
		try{
			
			int i = 0;
			while(iter.hasNext()){
				long item = iter.next();
				List<RecommendedItem> itemRecommendations = itemRecommender.mostSimilarItems(item, Integer.parseInt(args[1]));
				
				for(RecommendedItem recomItem : itemRecommendations){					
					StringBuilder str = new StringBuilder();
					str.append(item).append(",").append(recomItem.getItemID()).append(",").append(recomItem.getValue());
					//System.out.println(str);
					fw.write(str.toString());
					fw.newLine();
				}
				++i;
			}
			System.out.println("item cnt : " + i);
		}catch(IOException e){
			
		}finally{
			fw.close();
		}
	}
}
