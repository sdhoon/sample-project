import java.io.File;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.common.RandomUtils;



public class Evaluator {
	
	public static void main(String[] args) throws Exception{
		RandomUtils.useTestSeed();
		//DataModel model = new GroupLensDataModel(new File("ratings.dat"));
		DataModel model = new FileDataModel(new File("part-00000"));
		
		
		RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		
		RecommenderBuilder builder = new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model) throws TasteException{
				
				//user-based
				/*UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
				return new GenericUserBasedRecommender(model, neighborhood, similarity);*/
				
				//item-based
				//ItemSimilarity similarity = new PearsonCorrelationSimilarity(model); //53
				//ItemSimilarity similarity = new EuclideanDistanceSimilarity (model);//1.7
				//ItemSimilarity similarity = new TanimotoCoefficientSimilarity(model); //1.77
				ItemSimilarity similarity = new LogLikelihoodSimilarity(model);//1.77
				
				return new GenericItemBasedRecommender(model, similarity);
			}
		};
		
		
		
		double score = evaluator.evaluate(builder, null, model, 0.95, 1.0);
		System.out.println(score);
	}	
	
}
