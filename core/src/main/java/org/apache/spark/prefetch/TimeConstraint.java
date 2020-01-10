package org.apache.spark.prefetch;

import com.github.signaflo.timeseries.TimeSeries;
import com.github.signaflo.timeseries.forecast.Forecast;
import com.github.signaflo.timeseries.model.arima.Arima;
import com.github.signaflo.timeseries.model.arima.ArimaCoefficients;

import java.util.Arrays;
import java.util.List;

public class TimeConstraint {

    private Arima.FittingStrategy fittingStrategy;
    private ArimaCoefficients coefficients;

    public TimeConstraint() {
        fittingStrategy = Arima.FittingStrategy.CSSML;
        coefficients = ArimaCoefficients.builder().build();
    }


    protected List<Double> nextTimeWindowDataSize(List<Long> series) {
        double [] standardSeries = series.stream().mapToDouble(d ->d).toArray();
        TimeSeries timeSeries = TimeSeries.from(standardSeries);
        Arima arima = Arima.model(timeSeries, coefficients, fittingStrategy);
        Forecast forecast = arima.forecast(24);
        return null;
    }

}
