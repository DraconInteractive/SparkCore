# Budget tracking

Open `/budget/explorer` from the dashboard. Budget data is stored in MariaDB and
included in the existing database backup. It is not exposed through assistant tools.

## Deployment

Rebuild, run `./scripts/setup-db.sh` as the SPARK operating-system user, then restart.
Migration `027_budget.sql` adds four tables and setup grants access to them. Existing
expense data does not exist before this migration; other domains are unchanged.
Token-mode clients need `budget.read` and/or `budget.write`. Existing tokens do not
automatically gain these permissions. TCP consumers also need `notifications.read`.
The permission checkboxes are populated by the shared permission catalogue.

## Expenses

`POST /budget/expenses` creates a record, `GET /budget/expenses` lists records,
`GET /budget/expenses/{id}` retrieves one, `PATCH /budget/expenses/{id}` modifies
it, and `DELETE /budget/expenses/{id}?revision=...` deletes it.

```json
{
  "name": "Groceries",
  "amount_cents": 5500,
  "currency": "AUD",
  "gst_cents": 500,
  "spent_on": "2026-11-20",
  "category": "Food",
  "notes": "Weekly shop",
  "subscription_id": null
}
```

Amounts use integer cents, never floating-point arithmetic. `amount_cents` is the
**gross total including GST**, from 1 to 100,000,000 (1,000,000 currency units).
`gst_cents` is optional: null means unspecified, zero means an explicitly zero
GST entry, and a positive value cannot exceed the total. No GST rate is inferred.
The currency defaults to AUD; all accepted three-uppercase-letter codes use a
fixed 1/100 unit. Currency conversion and zero/three-decimal currency conventions
are not supported. Use separate currency totals rather than adding them together.
Refunds/negative expenses are not included in this first phase.

`spent_on` is a Perth accounting **date**, not a UTC timestamp, and defaults to
today in Perth. Accepted dates span 1970–2199. `name` is required; category and notes
default to empty strings. A supplied subscription link must exist and have the same
currency. Directly linking an expense does not advance the subscription schedule;
use Record payment when that is wanted.

Responses include `id`, `revision`, `created_at` and `updated_at`. PATCH requires
the current revision; DELETE requires it in the query. A stale revision returns
409. Reload and review instead of blindly overwriting. Unknown fields are rejected
with their field name; request bodies must be JSON, at most 16 KiB.

Listing supports inclusive `from`/`to` dates, exact `category`, `currency`, and
`subscription_id`, plus `limit` (1–100, default 50) and `offset` (default 0).
Results are ordered by accounting date then ID and include `has_more`.
`GET /budget/summary` supports the same date/category/currency filters and provides
actual totals, category subtotals and specified GST by currency. Unspecified GST
entries are counted separately rather than silently treated as zero tax.

## Subscriptions and payments

The same CRUD methods live under `/budget/subscriptions`. A subscription has name,
amount, optional GST, currency, category and notes, together with:

- `cadence`: weekly, fortnightly, monthly, quarterly or yearly.
- `next_billing_date`: required date, explicitly advanced after recording payment.
- `billing_day`: 1–31; defaults to the day of the initial next billing date. Used
  for monthly/quarterly/yearly recurrence. January 31 → February 28 → March 31.
- `status`: active (default), paused or cancelled.
- `end_date`: optional inclusive last billing date, otherwise null.
- `reminder_days`: null disables reminders; 0–365 enables a reminder that many
  days before the next billing date.

Changing `next_billing_date` resets `billing_day` to that date's day unless the
request explicitly includes `billing_day`. The explorer displays the anchor for
editing. Paused and cancelled subscriptions produce no forecast charges/reminders.
An end date earlier than the next billing date makes the schedule exhausted.

To confirm an actual payment:

```http
POST /budget/subscriptions/{id}/payments
Content-Type: application/json

{"revision":1,"scheduled_date":"2026-11-30","spent_on":"2026-11-30"}
```

Optional `amount_cents` and `gst_cents` override this payment, without changing the
subscription's future expected amount. If omitted, both use the subscription's
values; a reduced amount may therefore require an explicit GST override.
The request atomically creates a linked expense and advances **one** billing period.
It does not charge a bank account. If several periods are overdue, explicitly record
each payment or edit the next date to correct the schedule.

The unique subscription ID + scheduled date is the retry key. Repeated requests,
including concurrent requests, return `already_recorded: true` and the original
expense ID. Deleting an expense or subscription retains this small receipt, so a
retry cannot recreate deleted spending. A deleted expense is returned as null.
Use PATCH on an expense for corrections. Deleting a subscription leaves its historical
expense links intact, so those entries remain excluded from non-subscription forecasts.

## Forecasts

`GET /budget/forecast?mode=subscriptions` forecasts scheduled active subscriptions.
`mode=patterns` adds an extrapolation of non-subscription expenditure.

- `from` defaults to today Perth and must not be in the past.
- `to` is inclusive, defaults to `from + 29 days`; window maximum 366 days.
- `history_days`: 1–365, default 90. History ends yesterday Perth even when the
  requested forecast starts further in the future.
- `currency`: optional exact code filter; no cross-currency aggregation.

Pattern calculation per currency:

```
non-subscription expenses in trailing history / history_days * forecast_days
+ active subscription charges within the forecast window
```

Rounding happens at the final pattern total to the nearest cent. The denominator
includes zero-spend days. Future-dated expenses and all expenses with a subscription
link are excluded from the baseline to avoid double counting. Manual subscription
payments without links will still enter the baseline—link them for useful forecasts.
Charges are extrapolated forward from the current schedule, even when payment
confirmation is overdue; old charges before the window are listed separately and
not added. Forecasting never advances schedules or records spending.

Responses contain per-currency `subscription_cents`, `pattern_cents`, `total_cents`,
`history_non_subscription_cents`, and `history_expense_count`; the dated `charges`;
`overdue_subscriptions`; and explicit `assumptions`. Sparse history can understate
spending. This simple average does not model seasonality, price increases or missing
records. Summary/forecast queries reject more than 100,000 matching records or
forecast charges; narrow the date/currency filter when `too_many_records` is returned.

## Notifications

All changes append events in the same transaction as their data changes:

- `budget.expense.created`, `.updated`, `.deleted`
- `budget.subscription.created`, `.updated`, `.deleted`
- `budget.subscription.reminder`

Mutation events contain the record ID/revision and REST resource; clients fetch
fresh state. Payment recording emits both expense-created and subscription-updated.
Reminder payloads contain `subscription_id` and `scheduled_date`. Reminders become
due at Perth midnight, checked every 30 seconds, and catch up after server downtime.
They are emitted once per next billing occurrence, even if the reminder lead time
is edited; no acknowledgement is required. They do not automatically roll forward
while payments remain unconfirmed. Reconnect catchup follows the existing TCP feed.
Financial notifications require both `budget.read` and `notifications.read`.

## Validation

Build `budget_tests`, `authorization_tests`, and `spark_tests`, then run
`./scripts/test-budget.sh`. It provisions a disposable MariaDB, tests date boundaries,
GST constraints, revision conflicts, currency separation, forecasts, concurrent
payment deduplication, reminders, notifications and the API contract. It never uses
the configured main/test instances. `scripts/test-budget-page.py` exercises the UI
in headless Firefox using a mock API.
