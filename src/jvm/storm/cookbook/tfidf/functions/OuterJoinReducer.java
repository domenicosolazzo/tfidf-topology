package storm.cookbook.tfidf.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.MultiReducer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentMultiReducerContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class OuterJoinReducer implements MultiReducer<OuterJoinState> {
    private static final long serialVersionUID = 1L;

    @Override
    public void prepare(Map conf, TridentMultiReducerContext context) {

    }

    @Override
    public OuterJoinState init(TridentCollector collector) {
        return new OuterJoinState();
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public void execute(OuterJoinState state, int streamIndex,
                        TridentTuple input, TridentCollector collector) {
        state.addValues(streamIndex, input);
    }

    @Override
    public void complete(OuterJoinState state,
                         TridentCollector collector) {
        for(Values vals : state.join()){
            collector.emit(vals);
        }
    }

}
