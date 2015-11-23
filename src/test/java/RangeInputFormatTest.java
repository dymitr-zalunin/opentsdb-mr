import com.dz.tools.mr.RangeInputFormat;
import com.dz.tools.mr.RangeInputSplit;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by leon on 30/11/15.
 */
public class RangeInputFormatTest {

    @Test
    public void testGetSplits() throws Exception {
        long start=new DateTime(2011,1,1,0,0,0,0).getMillis()/1000;
        long stop=new DateTime(2011,1,31,0,0,0,0).getMillis()/1000;
        long span=36;

        Configuration configuration=new Configuration();
        configuration.setLong("start",start);
        configuration.setLong("stop",stop);
        configuration.setLong("span",span);
        configuration.setLong("rows.per.map",30000);
        JobContext jobContext=new JobContext(configuration, null);
        RangeInputFormat inputFormat=new RangeInputFormat();
        List<InputSplit> splits=inputFormat.getSplits(jobContext);
        assertEquals(3,splits.size());
        List<InputSplit> expected= Lists.<InputSplit>newArrayList(
                new RangeInputSplit(1293836400,1294916400, 36),
                new RangeInputSplit(1294916400, 1295996400, 36),
                new RangeInputSplit(1295996400, 1296428400, 36)
        );
        assertEquals(expected, splits);
    }
}