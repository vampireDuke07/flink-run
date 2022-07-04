import mockit.Expectations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Arrays;

/**
 * @author liangjianxiang
 * @date 2022/6/21 15:47
 */
public class a {
    private static Logger LOG = LogManager.getLogger(a.class);

    public static void main(String[] args) {
//        t1("nb");
        AA aa = new AA();
        String word = "nb";
        LOG.info("say thing: {}", AA.t());
        LOG.info("say thing: {}", word);
        new Expectations(aa){
            {
                AA.t();
                result = "ss";
            }
        };

        Assert.assertSame("s", aa.s);
//        Assert.assertEquals(aa.t(),"b");

    }

    @Test
    public void test3() throws FileNotFoundException {
        Logger LOG = LogManager.getLogger(a.class);
        String s = "n,b,a";
        s = null;

        try {
            String[] split = s.split(",");
            System.out.println(split.length + "\n" + Arrays.toString(Arrays.stream(split).toArray()));
        } catch (Exception e) {

            LOG.error("split failed",e);
        }

    }


}

