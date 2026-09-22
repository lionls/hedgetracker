import { useEffect, useRef } from 'react';
import {
  AreaSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  createChart,
} from 'lightweight-charts';
import { compact } from './numbers.js';

// The pane heights and the axis format a fund page's charts share. The dashboard
// writes money in the compact form everywhere else, and an axis needs the sign
// spelled out: the compact formatter writes a negative six-figure sum as
// "-1.24M", which on a price scale reads as a minus hanging off the number.
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
  timeScale: { borderColor: '#232a35', rightOffset: 2 },
  crosshair: { mode: CrosshairMode.Normal },
};

const MONEY_FORMAT = {
  type: 'custom',
  minMove: 1,
  formatter: (value) => `${value < 0 ? '−' : ''}$${compact(Math.abs(value))}`,
};

const KINDS = { area: AreaSeries, line: LineSeries, histogram: HistogramSeries };

// QuarterChart draws one panel of a fund's history: a list of series, each a kind,
// its options, the pane it belongs to, and its points. The series are described
// once per panel and only their data changes, so the chart, its panes and its
// prices scales are created once and survive every fund and quarter change —
// which is also why the descriptors have to keep their shape for the lifetime of
// the panel. A panel whose data is a single point is the caller's problem to
// substitute something else for: an axis with one tick says nothing.
export default function QuarterChart({ title, caption, series, tall = false }) {
  const holder = useRef(null);
  const chart = useRef(null);
  const drawn = useRef([]);

  useEffect(() => {
    const instance = createChart(holder.current, CHART_OPTIONS);
    drawn.current = series.map((entry) =>
      instance.addSeries(KINDS[entry.kind], { priceFormat: MONEY_FORMAT, ...entry.options }, entry.pane || 0),
    );
    // The estimate is the reason the upper pane exists, so it gets the height.
    if (series.some((entry) => entry.pane)) instance.panes()[0]?.setStretchFactor(2);
    chart.current = instance;
    return () => {
      chart.current = null;
      drawn.current = [];
      instance.remove();
    };
  }, []);

  useEffect(() => {
    if (!chart.current) return;
    series.forEach((entry, index) => drawn.current[index]?.setData(entry.data));
    chart.current.timeScale().fitContent();
  }, [series]);

  return (
    <figure className="chart-panel">
      <figcaption>
        <h3>{title}</h3>
        {caption ? <span className="muted">{caption}</span> : null}
      </figcaption>
      <div className={tall ? 'chart tall' : 'chart'} ref={holder} />
    </figure>
  );
}
