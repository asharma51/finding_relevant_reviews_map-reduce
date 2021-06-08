import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class UserToRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text user = new Text();
	private DoubleWritable rating = new DoubleWritable();
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  String [] ratingTuple=itr.nextToken().split(",");
    	  user.set(ratingTuple[0]);
    	  rating.set(Double.parseDouble(ratingTuple[2]));
    	  context.write(user, rating);
      }      
  }
}
