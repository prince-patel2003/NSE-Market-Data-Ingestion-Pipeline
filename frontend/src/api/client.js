import axios from "axios";

const baseURL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export const api = axios.create({ baseURL });

export async function searchSymbols(query) {
  const { data } = await api.get("/api/symbols", { params: { q: query } });
  return data;
}

export async function getQuote(symbol) {
  const { data } = await api.get(`/api/quote/${encodeURIComponent(symbol)}`);
  return data;
}

export async function getHistorical(symbol, { interval = "ONE_DAY", days = 90 } = {}) {
  const { data } = await api.get(`/api/historical/${encodeURIComponent(symbol)}`, {
    params: { interval, days },
  });
  return data;
}

export function extractErrorMessage(error) {
  return error?.response?.data?.detail || error.message || "Something went wrong";
}
