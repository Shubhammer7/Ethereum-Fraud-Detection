-- Joins ETH + token flows to find txs where both occurred

SELECT
    it.tx_hash,
    it.sender,
    it.receiver,
    it.value_eth,
    tt.token_symbol,
    tt.value_token,
    it.timestamp
FROM
    internal_transactions it
JOIN
    token_transfers tt ON it.tx_hash = tt.tx_hash
ORDER BY
    it.timestamp DESC;
