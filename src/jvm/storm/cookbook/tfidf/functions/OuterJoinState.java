package storm.cookbook.tfidf.functions;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * User: domenicosolazzo
 */
public class OuterJoinState {
    //2 columns, based on streamId. Each column contains the tuples from the given stream
    private HashMap<Integer, List<Object[]>> bothSides = new HashMap<Integer,List<Object[]>>();

    public void addValues(int streamId, TridentTuple input){
        if(!bothSides.keySet().contains(streamId)){
            if(bothSides.keySet().size() >= 2)
                throw new IllegalArgumentException("Outer join can only be performed between 2 streams");
            bothSides.put(streamId, new ArrayList<Object[]>());
        }
        bothSides.get(streamId).add(input.toArray());
    }

    //the shorter side is the LHS
    private int getLHS(){
        int len = Integer.MAX_VALUE;
        int index = 0;
        for(int id : bothSides.keySet()){
            if(bothSides.get(id).size() < len){
                len = bothSides.get(id).size();
                index = id;
            }
        }
        return index;
    }

    private int getRhs(int lhs){
        for(int test : bothSides.keySet()){
            if(test != lhs)
                return test;
        }
        throw new IllegalArgumentException("Can't find RHS!");
    }


    public List<Values> join(){
        List<Values> ret = new ArrayList<Values>();
        try{
            int lhsId = getLHS();
            int rhsId = getRhs(lhsId);
            for(Object[] lhs : bothSides.get(lhsId)){
                for(Object[] rhs : bothSides.get(rhsId)){
                    Values vals = new Values(lhs);
                    vals.addAll(Arrays.asList(rhs));
                    ret.add(vals);
                }
            }
        }catch(Exception e){}
        return ret;
    }

}
