package Mahout;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.ga.watchmaker.EvalMapper;

/*
 *@author Aishwarya Srinivasan
 */

public class MahoutRecco {

	
	public static void main(String[] args) throws IOException, TasteException{
		String fName = "/Users/hduser/tmp/ml-1m/ratings_csv.csv";
		File f = new File(fName);
		DataModel model = new FileDataModel(f);
		
		final UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		final UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, similarity, model);

		Recommender recommender = new GenericUserBasedRecommender (model, neighborhood, similarity);
		
		List<RecommendedItem> recommendations =
				recommender.recommend(67, 1);
				for (RecommendedItem recommendation : recommendations) {
				System.out.println("my reco : "+recommendation.getItemID());
				}
			
				
			RecommenderBuilder rb = new RecommenderBuilder() {
				
				@Override
				public Recommender buildRecommender(DataModel arg0) throws TasteException {
					String fName = "/Users/hduser/tmp/ml-1m/ratings_csv.csv";
					File f = new File(fName);
					DataModel model = null;
					try {
						model = new FileDataModel(f);
						UserSimilarity similarity = new PearsonCorrelationSimilarity (model);
						UserNeighborhood neighborhood =
						new NearestNUserNeighborhood (2, similarity, model);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					return new GenericUserBasedRecommender (model, neighborhood, similarity);
				}
			};	
			
			RecommenderEvaluator e = new AverageAbsoluteDifferenceRecommenderEvaluator();
			double score = e.evaluate(rb, null, model, 0.3, 1.0);
			System.out.println("Score : "+score);
				
	}
	
	
	public String recco(String userid) throws IOException, TasteException{
		
		String fName = "/Users/hduser/tmp/ml-1m/ratings_csv.csv";
		File f = new File(fName);
		DataModel model = new FileDataModel(f);
		int uid = Integer.parseInt(userid);
		final UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		final UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, similarity, model);
		String recco = new String();
		recco = "1";
		Recommender recommender = new GenericUserBasedRecommender (model, neighborhood, similarity);
		
		List<RecommendedItem> recommendations =
				recommender.recommend(uid, 1);
				for (RecommendedItem recommendation : recommendations) {
				
				Long temp = recommendation.getItemID();
				recco = temp.toString();
				
				}
				//System.out.println("my reco : "+recco);
				
				return recco.toString();
	}
	
	
}
