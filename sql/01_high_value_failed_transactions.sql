-- Finds ETH transactions that failed or were high value (> 50 ETH)

SELECT
    tx_hash, sender, receiver, value_eth, gas_used, is_error, tx_type, timestamp
FROM
    internal_transactions
WHERE
    tx_type IN ('High value transaction', 'Failed transaction')
ORDER BY
    timestamp DESC;
