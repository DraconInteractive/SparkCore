-- IDs match TCP hello identities. No registry FK: hello does not require registration.
CREATE TABLE IF NOT EXISTS pulse_subscriptions (
    device_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    subscribed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
