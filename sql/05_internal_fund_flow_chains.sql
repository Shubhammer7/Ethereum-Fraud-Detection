-- Detects internal ETH flows involving the same tx hash (multi-hop patterns)

SELECT
    tx_hash,
    from_address,
    to_address,
    value_eth,
    trace_id,
    call_type,
    timestamp
FROM
    eth_internal_txs
WHERE
    value_eth > 0.01
ORDER BY
    tx_hash, trace_id;
