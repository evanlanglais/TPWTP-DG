CREATE TABLE IF NOT EXISTS park_schedules (
    park_id       UUID        NOT NULL REFERENCES parks(id) ON DELETE CASCADE,
    date          DATE        NOT NULL,
    type          TEXT        NOT NULL,
    opening_time  TIMESTAMPTZ NOT NULL,
    closing_time  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (park_id, date, type, opening_time)
);

CREATE INDEX IF NOT EXISTS idx_park_schedules_park_date
    ON park_schedules (park_id, date);

-- Supports "is park open at time T?" queries from frontend
CREATE INDEX IF NOT EXISTS idx_park_schedules_park_times
    ON park_schedules (park_id, opening_time, closing_time);
