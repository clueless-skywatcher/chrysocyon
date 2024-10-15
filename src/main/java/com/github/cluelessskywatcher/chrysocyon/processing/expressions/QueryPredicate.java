package com.github.cluelessskywatcher.chrysocyon.processing.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import com.github.cluelessskywatcher.chrysocyon.planning.DatabasePlan;
import com.github.cluelessskywatcher.chrysocyon.processing.scans.IScan;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.data.DataField;

public class QueryPredicate {
    private List<PredicateTerm> terms = new ArrayList<>();

    public QueryPredicate() {}

    public QueryPredicate(PredicateTerm t) {
        terms.add(t);
    }

    public void conjoin(QueryPredicate predicate) {
        terms.addAll(predicate.terms);
    }

    public boolean isSatisfied(IScan s) {
        for (PredicateTerm t : terms) {
            if (!t.isSatisfied(s)) {
                return false;
            }
        }

        return true;
    }

    public QueryPredicate selectSubPredicate(TupleSchema schema) {
        QueryPredicate result = new QueryPredicate();

        for (PredicateTerm t : terms) {
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

        for (PredicateTerm t : terms) {
            if (!t.appliesTo(schema1) && !t.appliesTo(schema2) && t.appliesTo(newSchema)) {
                result.terms.add(t);
            }
        }

        if (result.terms.size() > 0)
            return result;
        
        return null;
    }

    public DataField equatesWithConstant(String fieldName) {
        for (PredicateTerm t : terms) {
            DataField field = t.equatesWithConstant(fieldName);
            if (field != null) return field;
        }

        return null;
    }

    public String equatesWithField(String fieldName) {
        for (PredicateTerm t : terms) {
            String s = t.equatesWithField(fieldName);
            if (s != null) return s;
        }

        return null;
    }

    public String toString() {
        Iterator<PredicateTerm> termIterator = terms.iterator();

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

    public int reductionFactor(DatabasePlan plan) {
        int rf = 1;
        for (PredicateTerm term : terms) {
            rf *= term.reductionFactor(plan);
        }
        return rf;
    }

    public boolean isEmpty() {
        return terms.size() == 0;
    }
}
