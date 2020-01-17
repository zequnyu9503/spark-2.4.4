package org.apache.spark.prefetch;

import com.github.signaflo.timeseries.TimeSeries;
import com.github.signaflo.timeseries.forecast.Forecast;
import com.github.signaflo.timeseries.model.arima.Arima;
import com.github.signaflo.timeseries.model.arima.ArimaOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DataSizeForecast {

    // Coefficient for moving-average.
    private int qMax = 3;
    // Coefficient for autoregressive.
    private int pMax = 3;
    // Coefficient for difference.
    private int dMax = 2;

    private List<ArimaOrder> orders;
    private ArimaOrder best;

    public DataSizeForecast() {
        orders = new ArrayList<>(qMax * pMax * dMax);
        for (int p = 0; p <= pMax; ++p)
            for (int d = 0; d <= dMax; ++d)
                for (int q = 0; q <= qMax; ++q)
                    orders.add(ArimaOrder.order(p, d, q));
    }

    protected Double forecastNext(Long [] series) {
        return forecastNextN(series, 1).get(0);
    }

    protected List<Double> forecastNextN(Long [] series, int steps) {
        double [] standardSeries = Arrays.stream(series).mapToDouble(i->i).toArray();
        if (best == null) bestOrder(standardSeries);
        TimeSeries timeSeries = TimeSeries.from(standardSeries);
        Arima arima = Arima.model(timeSeries, best);
        Forecast forecast = arima.forecast(steps);
        return forecast.pointEstimates().asList();
    }

    private void bestOrder(double [] series) {
        Iterator<ArimaOrder> orderIterator = orders.iterator();
        double bestAic = Double.MAX_VALUE;
        while (orderIterator.hasNext()) {
            ArimaOrder order = orderIterator.next();
            try {
                Arima arima = Arima.model(TimeSeries.from(series), order);
                double aicValue = arima.aic();
                if (!Double.isNaN(aicValue) && aicValue < bestAic) {
                    best = order;
                    bestAic = aicValue;
                }
            } catch (Exception e) {
                continue;
            }
        }
    }
}
