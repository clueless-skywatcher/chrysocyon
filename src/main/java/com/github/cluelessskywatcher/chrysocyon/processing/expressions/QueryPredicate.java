package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import com.github.cluelessskywatcher.chrysocyon.planning.OpPlan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class QueryPredicate {
    private List<PredTerm> terms = new ArrayList<>();

    public QueryPredicate() {}

    public QueryPredicate(PredTerm t) {
        terms.add(t);
    }

    public void conjoin(QueryPredicate predicate) {
        terms.addAll(predicate.terms);
    }

    public boolean isSatisfied(IScan s) {
        for (PredTerm t : terms) {
            if (!t.isSatisfied(s)) {
                return false;
            }
        }

        return true;
    }

    public QueryPredicate selectSubPredicate(TupleSchema schema) {
        QueryPredicate result = new QueryPredicate();

        for (PredTerm t : terms) {
            if (t.appliesTo(schema)) {
                result.terms.add(t);
            }
        }

        if (result.terms.size() > 0)
            return result;
        
        return null;
    }

    public QueryPredicate joinSubPredicate(TupleSchema schema1, TupleSchema schema2) {
        QueryPredicate result = new QueryPredicate();
        TupleSchema newSchema = new TupleSchema();

        newSchema.addAll(schema1);
        newSchema.addAll(schema2);

        for (PredTerm t : terms) {
            if (!t.appliesTo(schema1) && !t.appliesTo(schema2) && t.appliesTo(newSchema)) {
                result.terms.add(t);
            }
        }

        if (result.terms.size() > 0)
            return result;
        
        return null;
    }

    public DataField equatesWithConstant(String fieldName) {
        for (PredTerm t : terms) {
            DataField field = t.equatesWithConstant(fieldName);
            if (field != null) return field;
        }

        return null;
    }

    public String equatesWithField(String fieldName) {
        for (PredTerm t : terms) {
            String s = t.equatesWithField(fieldName);
            if (s != null) return s;
        }

        return null;
    }

    public String toString() {
        Iterator<PredTerm> termIterator = terms.iterator();

        if (!termIterator.hasNext()) return "";
        StringBuffer result = new StringBuffer(termIterator.next().toString());
        while (termIterator.hasNext()) {
            result.append(" and " + termIterator.next().toString());
        }
        return result.toString();
    }

    public boolean equals(Object o) {
        if (o instanceof QueryPredicate) {
            QueryPredicate other = (QueryPredicate) o;
            return this.terms.equals(other.terms);
        }
        return false;
    }

    public int reductionFactor(OpPlan plan) {
        int rf = 1;
        for (PredTerm term : terms) {
            rf *= term.reductionFactor(plan);
        }
        return rf;
    }
}
