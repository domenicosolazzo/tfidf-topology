package storm.cookbook.tfidf.state;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * User: domenicosolazzo
 */
public class StateUtils {

    public static String formatHour(Date date){
        return new SimpleDateFormat("yyyyMMddHH").format(date);
    }

}