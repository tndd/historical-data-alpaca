SELECT
    mddp.asset_id,
    mddp.category,
    mddp.time_frame,
    mddp.until,
    mddp.message,
    ast.status,
    ast.symbol
FROM market_data_dl_progress mddp
JOIN assets ast ON mddp.asset_id = ast.id
WHERE mddp.category = %s
AND mddp.time_frame = %s
AND ast.status = 'active';
