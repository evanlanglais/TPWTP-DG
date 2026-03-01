ALTER TABLE l2_processing_cursors
    ADD COLUMN IF NOT EXISTS logic_version INT NOT NULL DEFAULT 1;
