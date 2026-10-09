-- =============================================================================
-- 02_trading_calendar.sql — NSE equity-segment trading holidays and special sessions.
-- Idempotent (ON CONFLICT DO UPDATE). Add each new year's list from the NSE
-- holiday circular: https://www.nseindia.com/resources/exchange-communication-holidays
--
-- The calendar does not have to be complete for correctness: a weekday that is
-- missing here is simply requested once, and if the source has no candle for it
-- it is recorded in market.no_data_dates and never requested again. Keeping the
-- calendar accurate just saves API calls and makes gap reports meaningful.
-- =============================================================================

INSERT INTO market.trading_holidays (exchange, holiday_date, description) VALUES
    -- 2024
    ('NSE', '2024-01-22', 'Special holiday (Ram Mandir Pran Pratishtha)'),
    ('NSE', '2024-01-26', 'Republic Day'),
    ('NSE', '2024-03-08', 'Mahashivratri'),
    ('NSE', '2024-03-25', 'Holi'),
    ('NSE', '2024-03-29', 'Good Friday'),
    ('NSE', '2024-04-11', 'Id-ul-Fitr (Ramadan Eid)'),
    ('NSE', '2024-04-17', 'Shri Ram Navami'),
    ('NSE', '2024-05-01', 'Maharashtra Day'),
    ('NSE', '2024-05-20', 'General Parliamentary Elections (Mumbai)'),
    ('NSE', '2024-06-17', 'Bakri Id'),
    ('NSE', '2024-07-17', 'Moharram'),
    ('NSE', '2024-08-15', 'Independence Day'),
    ('NSE', '2024-10-02', 'Mahatma Gandhi Jayanti'),
    ('NSE', '2024-11-01', 'Diwali Laxmi Pujan'),
    ('NSE', '2024-11-15', 'Gurunanak Jayanti'),
    ('NSE', '2024-11-20', 'Maharashtra Assembly Elections'),
    ('NSE', '2024-12-25', 'Christmas'),
    -- 2025
    ('NSE', '2025-02-26', 'Mahashivratri'),
    ('NSE', '2025-03-14', 'Holi'),
    ('NSE', '2025-03-31', 'Id-ul-Fitr (Ramadan Eid)'),
    ('NSE', '2025-04-10', 'Shri Mahavir Jayanti'),
    ('NSE', '2025-04-14', 'Dr. Baba Saheb Ambedkar Jayanti'),
    ('NSE', '2025-04-18', 'Good Friday'),
    ('NSE', '2025-05-01', 'Maharashtra Day'),
    ('NSE', '2025-08-15', 'Independence Day'),
    ('NSE', '2025-08-27', 'Ganesh Chaturthi'),
    ('NSE', '2025-10-02', 'Mahatma Gandhi Jayanti / Dussehra'),
    ('NSE', '2025-10-21', 'Diwali Laxmi Pujan'),
    ('NSE', '2025-10-22', 'Diwali Balipratipada'),
    ('NSE', '2025-11-05', 'Prakash Gurpurb Sri Guru Nanak Dev'),
    ('NSE', '2025-12-25', 'Christmas'),
    -- 2026 (verify against the NSE circular)
    ('NSE', '2026-01-15', 'Municipal Corporation Elections (Maharashtra)'),
    ('NSE', '2026-01-26', 'Republic Day'),
    ('NSE', '2026-03-03', 'Holi'),
    ('NSE', '2026-03-26', 'Shri Ram Navami'),
    ('NSE', '2026-03-31', 'Shri Mahavir Jayanti'),
    ('NSE', '2026-04-03', 'Good Friday'),
    ('NSE', '2026-04-14', 'Dr. Baba Saheb Ambedkar Jayanti'),
    ('NSE', '2026-05-01', 'Maharashtra Day'),
    ('NSE', '2026-05-28', 'Bakri Id'),
    ('NSE', '2026-06-26', 'Muharram'),
    ('NSE', '2026-09-14', 'Ganesh Chaturthi'),
    ('NSE', '2026-10-02', 'Mahatma Gandhi Jayanti'),
    ('NSE', '2026-10-20', 'Dussehra'),
    ('NSE', '2026-11-10', 'Diwali Balipratipada'),
    ('NSE', '2026-11-24', 'Prakash Gurpurb Sri Guru Nanak Dev'),
    ('NSE', '2026-12-25', 'Christmas')
ON CONFLICT (exchange, holiday_date) DO UPDATE SET description = EXCLUDED.description;

INSERT INTO market.special_sessions (exchange, session_date, description) VALUES
    ('NSE', '2024-01-20', 'Saturday full trading session'),
    ('NSE', '2024-03-02', 'Saturday special session (DR site switchover)'),
    ('NSE', '2024-11-01', 'Muhurat trading'),
    ('NSE', '2025-02-01', 'Union Budget (Saturday)'),
    ('NSE', '2025-10-21', 'Muhurat trading'),
    ('NSE', '2026-02-01', 'Union Budget (Sunday)')
ON CONFLICT (exchange, session_date) DO UPDATE SET description = EXCLUDED.description;
