import { useEffect, useRef, useState } from "react";
import { searchSymbols } from "../api/client";

export default function SymbolSearch({ onSelect }) {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const containerRef = useRef(null);

  useEffect(() => {
    if (query.trim().length === 0) {
      setResults([]);
      return;
    }
    const handle = setTimeout(async () => {
      setLoading(true);
      try {
        const data = await searchSymbols(query.trim());
        setResults(data);
        setOpen(true);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 300);
    return () => clearTimeout(handle);
  }, [query]);

  useEffect(() => {
    function handleClickOutside(event) {
      if (containerRef.current && !containerRef.current.contains(event.target)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function handleSelect(result) {
    setQuery(result.symbol);
    setOpen(false);
    onSelect(result);
  }

  return (
    <div ref={containerRef} className="relative w-full max-w-md">
      <input
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onFocus={() => results.length > 0 && setOpen(true)}
        placeholder="Search NSE stock, e.g. RELIANCE, TCS, INFY"
        className="w-full rounded-lg border border-slate-700 bg-slate-900 px-4 py-2.5 text-sm text-slate-100 placeholder:text-slate-500 focus:border-sky-500 focus:outline-none"
      />
      {open && (loading || results.length > 0) && (
        <ul className="absolute z-10 mt-1 w-full max-h-72 overflow-y-auto rounded-lg border border-slate-700 bg-slate-900 shadow-xl">
          {loading && <li className="px-4 py-2 text-sm text-slate-400">Searching…</li>}
          {!loading &&
            results.map((r) => (
              <li
                key={r.token}
                onClick={() => handleSelect(r)}
                className="cursor-pointer px-4 py-2 text-sm hover:bg-slate-800"
              >
                <span className="font-medium text-slate-100">{r.symbol}</span>
                <span className="ml-2 text-slate-400">{r.name}</span>
              </li>
            ))}
        </ul>
      )}
    </div>
  );
}
