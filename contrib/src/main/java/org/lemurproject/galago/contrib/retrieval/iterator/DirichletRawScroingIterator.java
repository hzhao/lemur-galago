package org.lemurproject.galago.contrib.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * Created by hzhao on 5/22/15.
 */

@RequiredStatistics(statistics = {"collectionLength", "nodeFrequency", "maximumCount"})
@RequiredParameters(parameters = {"mu"})
public class DirichletRawScroingIterator extends ScoringFunctionIterator {
    // stats
    private final double mu;
    private final double background;

    public DirichletRawScroingIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
            throws IOException {
        super(p, ls, it);

        // stats
        mu = p.get("mu", 1500D);
        long collectionLength = p.getLong("collectionLength");
        long collectionFrequency = p.getLong("nodeFrequency");
        background = (collectionFrequency > 0)
                ? (double) collectionFrequency / (double) collectionLength
                : 0.5 / (double) collectionLength;
    }

    @Override
    public double score(ScoringContext c) {
        int count = ((CountIterator) iterator).count(c);
        int length = this.lengthsIterator.length(c);
        return dirichletScore(count, length);
    }

    private double dirichletScore(double count, double length) {
        double numerator = count + (mu * background);
        double denominator = length + mu;
        return Math.log(numerator) - Math.log(denominator);
    }
}
