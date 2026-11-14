-- Transfer-level metadata, decoded from the packet payload and the
-- source/destination transactions, so the metrics surface can read amounts,
-- tokens, fees and application type without re-deriving them off-chain.

ALTER TABLE ibcv2_transfers ADD COLUMN application TEXT;
ALTER TABLE ibcv2_transfers ADD COLUMN sender TEXT;
ALTER TABLE ibcv2_transfers ADD COLUMN receiver TEXT;
ALTER TABLE ibcv2_transfers ADD COLUMN fast_fill BOOLEAN;

ALTER TABLE ibcv2_transfers ADD COLUMN transfer_amount NUMERIC;
ALTER TABLE ibcv2_transfers ADD COLUMN denom TEXT;

ALTER TABLE ibcv2_transfers ADD COLUMN token_origin TEXT;
ALTER TABLE ibcv2_transfers ADD COLUMN token_origin_decimals INT;
ALTER TABLE ibcv2_transfers ADD COLUMN token_destination TEXT;
ALTER TABLE ibcv2_transfers ADD COLUMN token_destination_decimals INT;

ALTER TABLE ibcv2_transfers ADD COLUMN fee_origin NUMERIC;
ALTER TABLE ibcv2_transfers ADD COLUMN fee_origin_denom TEXT;
ALTER TABLE ibcv2_transfers ADD COLUMN fee_destination NUMERIC;
ALTER TABLE ibcv2_transfers ADD COLUMN fee_destination_denom TEXT;
