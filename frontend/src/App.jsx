import { useCallback, useEffect, useState } from "react";
import SymbolSearch from "./components/SymbolSearch";
import QuotePanel from "./components/QuotePanel";
import CandleChart from "./components/CandleChart";
import { extractErrorMessage, getHistorical, getQuote } from "./api/client";

const INTERVALS = [
  { value: "ONE_DAY", label: "Daily" },
  { value: "ONE_HOUR", label: "1 Hour" },
  { value: "FIFTEEN_MINUTE", label: "15 Min" },
  { value: "FIVE_MINUTE", label: "5 Min" },
];

const QUOTE_REFRESH_MS = 15000;

export default function App() {
  const [symbol, setSymbol] = useState(null);
  const [quote, setQuote] = useState(null);
  const [candles, setCandles] = useState(null);
  const [interval, setIntervalValue] = useState("ONE_DAY");
  const [days, setDays] = useState(90);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const refreshQuote = useCallback(async (sym) => {
    try {
      const data = await getQuote(sym);
      setQuote(data);
    } catch (err) {
      setError(extractErrorMessage(err));
    }
  }, []);

  const loadHistorical = useCallback(async (sym, intervalValue, daysValue) => {
    setLoading(true);
    setError("");
    try {
      const data = await getHistorical(sym, { interval: intervalValue, days: daysValue });
      setCandles(data.candles);
    } catch (err) {
      setError(extractErrorMessage(err));
      setCandles(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!symbol) return;
    refreshQuote(symbol.symbol);
    const id = setInterval(() => refreshQuote(symbol.symbol), QUOTE_REFRESH_MS);
    return () => clearInterval(id);
  }, [symbol, refreshQuote]);

  useEffect(() => {
    if (!symbol) return;
    loadHistorical(symbol.symbol, interval, days);
  }, [symbol, interval, days, loadHistorical]);

  function handleSelect(result) {
    setQuote(null);
    setCandles(null);
    setSymbol(result);
  }

  return (
    <div className="mx-auto min-h-screen max-w-4xl px-4 py-10">
      <header className="mb-8">
        <h1 className="text-2xl font-semibold text-slate-100">NSE Market Dashboard</h1>
        <p className="mt-1 text-sm text-slate-400">
          Live quotes and historical candles for NSE stocks, powered by the Angel One SmartAPI.
        </p>
      </header>

      <SymbolSearch onSelect={handleSelect} />

      {error && (
        <div className="mt-4 rounded-lg border border-rose-800 bg-rose-950/50 px-4 py-2 text-sm text-rose-300">
          {error}
        </div>
      )}

      {symbol && (
        <div className="mt-6 space-y-6">
          <QuotePanel quote={quote} />

          <div className="flex flex-wrap items-center gap-3">
            <div className="flex gap-1 rounded-lg border border-slate-700 bg-slate-900 p-1">
              {INTERVALS.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => setIntervalValue(opt.value)}
                  className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                    interval === opt.value
                      ? "bg-sky-600 text-white"
                      : "text-slate-400 hover:text-slate-100"
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
            <label className="flex items-center gap-2 text-xs text-slate-400">
              Lookback (days)
              <input
                type="number"
                min={1}
                max={365}
                value={days}
                onChange={(e) => setDays(Number(e.target.value) || 1)}
                className="w-20 rounded-md border border-slate-700 bg-slate-900 px-2 py-1 text-slate-100 focus:border-sky-500 focus:outline-none"
              />
            </label>
            {loading && <span className="text-xs text-slate-500">Loading chart…</span>}
          </div>

          <CandleChart candles={candles} />
        </div>
      )}

      {!symbol && (
        <p className="mt-10 text-center text-sm text-slate-500">
          Search for a stock above to see its live quote and price history.
        </p>
      )}
    </div>
  );
}
