import { useEffect, useRef } from 'react';
import { CandlestickSeries, ColorType, CrosshairMode, HistogramSeries, createChart } from 'lightweight-charts';

// One chart instance is created for the lifetime of the panel; symbol and range
// changes only replace the series data, so nothing is torn down or relaid out.
const CHART_OPTIONS = {
  autoSize: true,
  layout: {
    background: { type: ColorType.Solid, color: '#12161d' },
    textColor: '#8b96a5',
    attributionLogo: false,
  },
  grid: {
    vertLines: { color: 'rgba(42, 49, 60, 0.5)' },
    horzLines: { color: 'rgba(42, 49, 60, 0.5)' },
  },
  rightPriceScale: { borderColor: '#232a35' },
  timeScale: { borderColor: '#232a35', rightOffset: 3 },
  crosshair: { mode: CrosshairMode.Normal },
};

const CANDLE_OPTIONS = {
  upColor: '#26a69a',
  downColor: '#ef5350',
  borderVisible: false,
  wickUpColor: '#26a69a',
  wickDownColor: '#ef5350',
};

const VOLUME_OPTIONS = {
  priceFormat: { type: 'volume' },
  priceLineVisible: false,
  lastValueVisible: false,
};

export default function Chart({ candles, volume }) {
  const holder = useRef(null);
  const chart = useRef(null);
  const price = useRef(null);
  const bars = useRef(null);

  useEffect(() => {
    const instance = createChart(holder.current, CHART_OPTIONS);
    price.current = instance.addSeries(CandlestickSeries, CANDLE_OPTIONS, 0);
    bars.current = instance.addSeries(HistogramSeries, VOLUME_OPTIONS, 1);
    chart.current = instance;
    return () => {
      chart.current = null;
      price.current = null;
      bars.current = null;
      instance.remove();
    };
  }, []);

  useEffect(() => {
    if (!chart.current) return;
    price.current.setData(candles);
    bars.current.setData(volume);
    chart.current.timeScale().fitContent();
  }, [candles, volume]);

  return <div className="chart" ref={holder} />;
}
