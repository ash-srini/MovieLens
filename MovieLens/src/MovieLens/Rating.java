package MovieLens;


/*
 *@author Aishwarya Srinivasan
 */

public class Rating {

	/**
	 * Ratings object
	 * @param userID
	 * @param movieID
	 * @param rating
	 * @param timestamp
	 */

	
	private String userID;
	private String movieID;
	private String rating;
	private String timestamp;

	
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getMovieID() {
		return movieID;
	}
	public void setMovieID(String movieID) {
		this.movieID = movieID;
	}
	public String getRating() {
		return rating;
	}
	public void setRating(String rating) {
		this.rating = rating;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}


}
