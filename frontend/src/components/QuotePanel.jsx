export default function QuotePanel({ quote }) {
  if (!quote) return null;
  const isUp = quote.change >= 0;

  const stats = [
    { label: "Open", value: quote.open },
    { label: "High", value: quote.high },
    { label: "Low", value: quote.low },
    { label: "Prev. Close", value: quote.close },
  ];

  return (
    <div className="rounded-xl border border-slate-700 bg-slate-900 p-6">
      <div className="flex items-baseline justify-between">
        <div>
          <h2 className="text-xl font-semibold text-slate-100">{quote.symbol}</h2>
          <p className="text-xs uppercase tracking-wide text-slate-500">{quote.exchange}</p>
        </div>
        <div className="text-right">
          <p className="text-3xl font-bold text-slate-100">₹{quote.ltp.toFixed(2)}</p>
          <p className={isUp ? "text-emerald-400" : "text-rose-400"}>
            {isUp ? "▲" : "▼"} {quote.change.toFixed(2)} ({quote.change_percent.toFixed(2)}%)
          </p>
        </div>
      </div>
      <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
        {stats.map((s) => (
          <div key={s.label} className="rounded-lg bg-slate-800/60 px-3 py-2">
            <p className="text-xs text-slate-500">{s.label}</p>
            <p className="text-sm font-medium text-slate-200">₹{s.value.toFixed(2)}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
