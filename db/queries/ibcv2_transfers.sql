-- name: UpdateTransferState :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), status=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: UpdateTransferRecvTx :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), recv_tx_hash=$1, recv_tx_time=$2, recv_tx_relayer_address=$3
WHERE source_chain_id=$4 AND packet_source_client_id=$5 AND packet_sequence_number=$6;

-- name: UpdateTransferRecvTxGasCostUSD :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), recv_tx_gas_cost_usd=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: UpdateTransferWriteAckTx :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), write_ack_tx_hash=$1, write_ack_tx_time=$2, write_ack_status=$3
WHERE source_chain_id=$4 AND packet_source_client_id=$5 AND packet_sequence_number=$6;

-- name: UpdateTransferAckTx :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), ack_tx_hash=$1, ack_tx_time=$2, ack_tx_relayer_address=$3
WHERE source_chain_id=$4 AND packet_source_client_id=$5 AND packet_sequence_number=$6;

-- name: UpdateTransferAckTxGasCostUSD :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), ack_tx_gas_cost_usd=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: UpdateTransferTimeoutTx :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), timeout_tx_hash=$1, timeout_tx_time=$2, timeout_tx_relayer_address=$3
WHERE source_chain_id=$4 AND packet_source_client_id=$5 AND packet_sequence_number=$6;

-- name: UpdateTransferTimeoutTxGasCostUSD :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), timeout_tx_gas_cost_usd=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: UpdateTransferSourceTxFinalizedTime :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), source_tx_finalized_time=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: UpdateTransferWriteAckTxFinalizedTime :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), write_ack_tx_finalized_time=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: GetUnfinishedIBCV2Transfers :many
SELECT * FROM ibcv2_transfers
WHERE status!='COMPLETE_WITH_ACK' AND status!='COMPLETE_WITH_TIMEOUT' AND status!='FAILED' and status!='COMPLETE_WITH_WRITE_ACK_SUCCESS' and status!='COMPLETE_WITH_WRITE_ACK_ERROR';

-- name: InsertIBCV2Transfer :exec
INSERT INTO ibcv2_transfers (
    source_chain_id,
    destination_chain_id,
    source_tx_hash,
    source_tx_time,
    packet_sequence_number,
    packet_source_client_id,
    packet_destination_client_id,
    packet_timeout_timestamp
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: ClearRecvTx :exec
UPDATE ibcv2_transfers 
set recv_tx_hash=null, recv_tx_time=null
WHERE source_chain_id=$1 AND packet_source_client_id=$2 AND packet_sequence_number=$3;

-- name: ClearAckTx :exec
UPDATE ibcv2_transfers 
set ack_tx_hash=null, ack_tx_time=null
WHERE source_chain_id=$1 AND packet_source_client_id=$2 AND packet_sequence_number=$3;

-- name: ClearTimeoutTx :exec
UPDATE ibcv2_transfers 
set timeout_tx_hash=null, timeout_tx_time=null
WHERE source_chain_id=$1 AND packet_source_client_id=$2 AND packet_sequence_number=$3;

-- name: GetTransfersBySourceTx :many
SELECT * FROM ibcv2_transfers
WHERE source_chain_id = $1 AND source_tx_hash = $2;

-- name: InsertRelayRequest :exec
INSERT INTO ibcv2_relay_requests (source_chain_id, source_tx_hash)
VALUES ($1, $2)
ON CONFLICT (source_chain_id, source_tx_hash) DO NOTHING;

-- name: GetRelayRequest :one
SELECT * FROM ibcv2_relay_requests
WHERE source_chain_id = $1 AND source_tx_hash = $2;

-- name: ListRelayStatuses :many
SELECT unnest(enum_range(NULL::ibcv2_relay_status))::text AS status;

-- name: CountTransfersByChainPairAndStatus :many
SELECT source_chain_id, destination_chain_id, status, COUNT(*)::bigint AS count
FROM ibcv2_transfers
GROUP BY source_chain_id, destination_chain_id, status;

-- name: UpdateTransferMetadata :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(),
    application=$1,
    sender=$2,
    receiver=$3,
    transfer_amount=$4,
    denom=$5,
    token_origin=$6,
    token_origin_decimals=$7
WHERE source_chain_id=$8 AND packet_source_client_id=$9 AND packet_sequence_number=$10;

-- name: UpdateTransferSourceFee :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), fee_origin=$1, fee_origin_denom=$2
WHERE source_chain_id=$3 AND packet_source_client_id=$4 AND packet_sequence_number=$5;

-- name: UpdateTransferDestinationToken :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), token_destination=$1, token_destination_decimals=$2
WHERE source_chain_id=$3 AND packet_source_client_id=$4 AND packet_sequence_number=$5;

-- name: UpdateTransferDestinationFee :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), fee_destination=$1, fee_destination_denom=$2
WHERE source_chain_id=$3 AND packet_source_client_id=$4 AND packet_sequence_number=$5;

-- name: UpdateTransferFastFill :exec
UPDATE ibcv2_transfers
SET updated_at=NOW(), fast_fill=$1
WHERE source_chain_id=$2 AND packet_source_client_id=$3 AND packet_sequence_number=$4;

-- name: SumTransferVolumeByDenom :many
SELECT denom,
       token_origin_decimals,
       SUM(transfer_amount)::numeric AS total_amount,
       COUNT(*)::bigint AS transfers
FROM ibcv2_transfers
WHERE transfer_amount IS NOT NULL
  AND denom IS NOT NULL
  AND source_tx_time >= $1
  AND status IN ('COMPLETE_WITH_ACK', 'COMPLETE_WITH_WRITE_ACK_SUCCESS')
GROUP BY denom, token_origin_decimals
ORDER BY transfers DESC;
