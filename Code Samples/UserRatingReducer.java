import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private DoubleWritable average = new DoubleWritable();
	
  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	  double sum = 0;
	  double avg=0;
	  int count=0;
      for (DoubleWritable val : values) {
        sum += val.get();
        count+=1;
      }
      avg=sum/count;
      average.set(avg);
      context.write(key, average);
    }
}