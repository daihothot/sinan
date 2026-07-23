ALTER TABLE inbound_admissions
ADD COLUMN raw_payload_length INTEGER CHECK (
  raw_payload_length IS NULL OR raw_payload_length >= 0
);

CREATE TRIGGER trg_inbound_admissions_raw_payload_length_immutable
BEFORE UPDATE OF raw_payload_length ON inbound_admissions
WHEN OLD.raw_payload_length IS NOT NEW.raw_payload_length
BEGIN
  SELECT RAISE(ABORT, 'inbound_admissions raw payload length is immutable');
END;
