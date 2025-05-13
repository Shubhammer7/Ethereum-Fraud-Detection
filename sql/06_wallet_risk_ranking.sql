-- Risk ranking: wallets involved in high-risk txs

SELECT
    it.sender,
    COUNT(*) FILTER (WHERE tx_type = 'Failed transaction') AS failed_count,
    COUNT(*) FILTER (WHERE tx_type = 'High value transaction') AS high_value_count,
    COUNT(*) FILTER (WHERE tx_type = 'High gas transaction') AS gas_flag_count,
    COUNT(*) AS total_tx_count
FROM internal_transactions it
GROUP BY it.sender
ORDER BY failed_count DESC, high_value_count DESC;
