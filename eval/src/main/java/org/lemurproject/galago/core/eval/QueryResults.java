/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import java.util.ArrayList;
import java.util.List;

/**
 * Stores a list of returned ScoredDocuments for a query.
 *
 * @author sjh
 */
public class QueryResults {

  private String query;
  private List<EvalDoc> rankedList;

  public QueryResults(List<? extends EvalDoc> rankedList) {
    this("unknown-id", rankedList);
  }
  public QueryResults(String query, List<? extends EvalDoc> rankedList) {
    this.query = query;
    this.rankedList = new ArrayList<>(rankedList);
  }

  public Iterable<EvalDoc> getIterator() {
    return rankedList;
  }

  public int size() {
    return rankedList.size();
  }
}
