import java.io.File;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;


public class SVDRecomender {
	
	public static void main(String[] args) throws Exception{
		DataModel model = new FileDataModel(new File("part-00000"));
		ALSWRFactorizer als = new ALSWRFactorizer(model, 3, 0.65, 10);
		
		Recommender recommender = new SVDRecommender(model, als);
		
		for(RecommendedItem item : recommender.recommend(7593914, 10)){
			
			System.out.println(item);
			
		}
		
	}

}
