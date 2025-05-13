-- Summarizes behavior of each wallet (tx count, total sent/received, errors)

SELECT
    sender AS wallet_address,
    COUNT(*) AS sent_count,
    SUM(value_eth) AS total_sent_eth,
    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS failed_sent,
    MAX(value_eth) AS max_sent
FROM internal_transactions
GROUP BY sender
ORDER BY total_sent_eth DESC;
