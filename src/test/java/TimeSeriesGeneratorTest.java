import com.dz.tools.TimeSeries;
import com.dz.tools.TimeSeriesGenerator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by leon on 29/11/15.
 */
public class TimeSeriesGeneratorTest {

    @Test
    public void testGenerateTimeSeries() throws Exception {
            long start=new DateTime(2011,1,1,0,0,0,0).getMillis()/1000;
            long stop=new DateTime(2011,1,31,0,0,0,0).getMillis()/1000;
            long span=36;
        TimeSeriesGenerator generator=new TimeSeriesGenerator();
        TimeSeries ts=generator.generateTimeSeries(start,stop,span,"", Maps.<String, String>newHashMap());
        assertEquals(72000,ts.getData_points().size());
        assertEquals(stop-span ,Iterables.getLast(ts.getData_points()).timestamp);
        assertEquals(start ,Iterables.getFirst(ts.getData_points(),null).timestamp);
    }

}
