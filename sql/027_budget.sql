-- Amounts in the JSON document are validated integer cents, never floating point.
CREATE TABLE IF NOT EXISTS budget_expenses (
 id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 data JSON NOT NULL,
 accounting_date DATE NOT NULL,
 revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
 created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 INDEX budget_expense_date(accounting_date,id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS budget_subscriptions LIKE budget_expenses;
-- Retained after expense/subscription deletion so a retry cannot record a payment twice.
CREATE TABLE IF NOT EXISTS budget_payments (
 subscription_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 scheduled_date DATE NOT NULL,
 expense_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 PRIMARY KEY(subscription_id,scheduled_date)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS budget_reminders (
 subscription_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 scheduled_date DATE NOT NULL,
 PRIMARY KEY(subscription_id,scheduled_date)
) ENGINE=InnoDB;
