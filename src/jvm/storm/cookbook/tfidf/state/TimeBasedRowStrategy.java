package storm.cookbook.tfidf.state;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import backtype.storm.utils.Time;

import trident.cassandra.CassandraState.Options;
import trident.cassandra.RowKeyStrategy;

/**
 * User: domenicosolazzo
 */
public class TimeBasedRowStrategy implements RowKeyStrategy, Serializable {

    private static final long serialVersionUID = 6981400531506165681L;


    @Override
    public <T> String getRowKey(List<List<Object>> keys, CassandraState.Options<T> options) {
        return options.rowKey + StateUtils.formatHour(new Date(Time.currentTimeMillis()));
    }

}