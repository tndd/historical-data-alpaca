UPDATE alpaca_market_db.market_data_dl_progress
SET
    until=%s,
    message=%s
WHERE asset_id=%s
AND category=%s
AND time_frame=%s;
