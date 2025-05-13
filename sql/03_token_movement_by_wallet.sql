-- Calculates total token volume sent/received by wallets with known labels

SELECT
    a.label,
    t.from_address,
    COUNT(*) AS sent_tx_count,
    SUM(value_token) AS total_tokens_sent,
    t.token_symbol
FROM
    token_transfers t
LEFT JOIN
    address_labels a ON t.from_address = a.address
GROUP BY a.label, t.from_address, t.token_symbol
ORDER BY total_tokens_sent DESC;
