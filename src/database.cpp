#include "database.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include "pulse.hpp"
#include "calendar.hpp"
#include "daily_tasks.hpp"
#include "files.hpp"
#include <mysql.h>
#include <array>
#include <charconv>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

namespace spark {
namespace {
using nlohmann::json;
using Parameters = std::vector<std::optional<std::string>>;
std::string env(const char* name, const std::string& fallback = {}) {
    const char* value = std::getenv(name);
    return value ? value : fallback;
}
struct Library {
    Library() { if (mysql_library_init(0, nullptr, nullptr)) throw DatabaseError("Cannot initialize MariaDB connector"); }
    ~Library() { mysql_library_end(); }
};
struct Connection {
    std::unique_ptr<MYSQL, decltype(&mysql_close)> handle{nullptr, mysql_close};
    explicit Connection(const DatabaseConfig& config) {
        static Library library;
        handle.reset(mysql_init(nullptr));
        if (!handle) throw DatabaseError("Cannot allocate MariaDB connection");
        unsigned int timeout = 5;
        mysql_options(handle.get(), MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        mysql_options(handle.get(), MYSQL_OPT_READ_TIMEOUT, &timeout);
        mysql_options(handle.get(), MYSQL_OPT_WRITE_TIMEOUT, &timeout);
        if (!mysql_real_connect(handle.get(), config.host.c_str(), config.user.c_str(), config.password.c_str(),
            config.database.c_str(), config.port, config.socket.empty() ? nullptr : config.socket.c_str(), 0))
            throw DatabaseError("Cannot connect to MariaDB (check service, credentials, and database)");
        if (mysql_set_character_set(handle.get(), "utf8mb4")) throw DatabaseError("Cannot set MariaDB character set");
        query("SET time_zone = '+00:00'");
        query("SET SESSION innodb_lock_wait_timeout = 5");
    }
    void query(const char* sql) {
        if (mysql_query(handle.get(), sql)) throw DatabaseError("MariaDB statement failed");
    }
};
struct Transaction {
    Connection& connection;
    bool committed = false;
    explicit Transaction(Connection& c) : connection(c) { connection.query("START TRANSACTION"); }
    ~Transaction() { if (!committed) mysql_rollback(connection.handle.get()); }
    void commit() {
        if (mysql_commit(connection.handle.get())) throw DatabaseError("MariaDB commit failed");
        committed = true;
    }
};
struct Statement {
    std::unique_ptr<MYSQL_STMT, decltype(&mysql_stmt_close)> handle{nullptr, mysql_stmt_close};
    Parameters values;
    std::vector<MYSQL_BIND> binds;
    Statement(Connection& connection, const std::string& sql, Parameters parameters = {}) : values(std::move(parameters)) {
        handle.reset(mysql_stmt_init(connection.handle.get()));
        if (!handle || mysql_stmt_prepare(handle.get(), sql.c_str(), sql.size()))
            throw DatabaseError("Cannot prepare MariaDB statement (check schema and permissions)");
        binds.resize(values.size());
        for (std::size_t i = 0; i < values.size(); ++i) {
            binds[i] = {};
            binds[i].buffer_type = values[i] ? MYSQL_TYPE_STRING : MYSQL_TYPE_NULL;
            if (values[i]) {
                binds[i].buffer = values[i]->data();
                binds[i].buffer_length = values[i]->size();
            }
        }
        if (!binds.empty() && mysql_stmt_bind_param(handle.get(), binds.data()))
            throw DatabaseError("Cannot bind MariaDB parameters");
    }
    bool execute(bool allow_duplicate = false) {
        if (mysql_stmt_execute(handle.get())) {
            if (allow_duplicate && mysql_stmt_errno(handle.get()) == 1062) return false;
            throw DatabaseError("MariaDB operation failed");
        }
        return true;
    }
    json rows() {
        std::array<char, 8192> buffer{};
        unsigned long length = 0;
        MYSQL_BIND result{};
        result.buffer_type = MYSQL_TYPE_STRING;
        result.buffer = buffer.data();
        result.buffer_length = buffer.size();
        result.length = &length;
        if (mysql_stmt_bind_result(handle.get(), &result) || mysql_stmt_store_result(handle.get()))
            throw DatabaseError("Cannot read MariaDB JSON result");
        json records = json::array();
        for (;;) {
            const int status = mysql_stmt_fetch(handle.get());
            if (status == MYSQL_NO_DATA) break;
            if (status != 0 && status != MYSQL_DATA_TRUNCATED) throw DatabaseError("Invalid MariaDB JSON result");
            if (length > buffer.size()) {
                if (length > 1048576) throw DatabaseError("MariaDB JSON result exceeds limit");
                std::vector<char> expanded(length);
                MYSQL_BIND column{};
                column.buffer_type = MYSQL_TYPE_STRING;
                column.buffer = expanded.data();
                column.buffer_length = expanded.size();
                if (mysql_stmt_fetch_column(handle.get(), &column, 0, 0)) throw DatabaseError("Cannot read full MariaDB JSON result");
                records.push_back(json::parse(expanded.begin(), expanded.end()));
            } else records.push_back(json::parse(buffer.data(), buffer.data() + length));
        }
        return records;
    }
    std::optional<json> device(const std::string& id) {
        std::array<std::array<char, 46>, 2> buffers{};
        std::array<unsigned long, 2> lengths{};
        std::array<my_bool, 2> nulls{};
        std::array<MYSQL_BIND, 2> results{};
        for (std::size_t i = 0; i < results.size(); ++i) {
            results[i].buffer_type = MYSQL_TYPE_STRING;
            results[i].buffer = buffers[i].data();
            results[i].buffer_length = buffers[i].size();
            results[i].length = &lengths[i];
            results[i].is_null = &nulls[i];
        }
        if (mysql_stmt_bind_result(handle.get(), results.data()) || mysql_stmt_store_result(handle.get()))
            throw DatabaseError("Cannot read MariaDB device");
        const int status = mysql_stmt_fetch(handle.get());
        if (status == MYSQL_NO_DATA) return std::nullopt;
        if (status != 0) throw DatabaseError("Invalid MariaDB device result");
        json record = {{"id", id}, {"local_ip", std::string(buffers[0].data(), lengths[0])}, {"external_ip", nullptr}};
        if (!nulls[1]) record["external_ip"] = std::string(buffers[1].data(), lengths[1]);
        return record;
    }
};
std::optional<std::string> external(const json& device) {
    if (device["external_ip"].is_null()) return std::nullopt;
    return device["external_ip"].get<std::string>();
}
void event(Connection& connection, const char* action, const json& device) {
    Statement statement(connection,
        "INSERT INTO device_events (device_id, action, local_ip, external_ip) VALUES (?, ?, ?, ?)",
        {device["id"].get<std::string>(), std::string(action), device["local_ip"].get<std::string>(), external(device)});
    statement.execute();
}
std::optional<json> lock_device(Connection& connection, const std::string& id) {
    Statement statement(connection, "SELECT local_ip, external_ip FROM devices WHERE id = ? FOR UPDATE", {id});
    statement.execute();
    return statement.device(id);
}
}
DatabaseConfig DatabaseConfig::from_environment() {
    DatabaseConfig config;
    config.host = env("SPARK_DB_HOST", "localhost");
    config.user = env("SPARK_DB_USER", env("USER", "spark"));
    config.password = env("SPARK_DB_PASSWORD");
    config.database = env("SPARK_DB_NAME", "spark");
    config.socket = env("SPARK_DB_SOCKET");
    const auto port = env("SPARK_DB_PORT", "3306");
    const auto parsed = std::from_chars(port.data(), port.data() + port.size(), config.port);
    if (parsed.ec != std::errc{} || parsed.ptr != port.data() + port.size() || config.port < 1 || config.port > 65535)
        throw DatabaseError("SPARK_DB_PORT must be an integer from 1 to 65535");
    return config;
}
DeviceStore::DeviceStore(DatabaseConfig config) : config_(std::move(config)) {}
void DeviceStore::verify() const {
    Connection connection(config_);
    // Preparing these queries also verifies that the expected schema is present.
    {
        Statement devices(connection, "SELECT id, local_ip, external_ip, created_at, updated_at FROM devices LIMIT 0");
        devices.execute();
    }
    {
        Statement events(connection, "SELECT event_id, device_id, action, local_ip, external_ip, recorded_at FROM device_events LIMIT 0");
        events.execute();
    }
}
std::optional<json> DeviceStore::get(const std::string& id) const {
    Connection connection(config_);
    Statement statement(connection,
        "SELECT JSON_OBJECT('id',d.id,'local_ip',d.local_ip,'external_ip',d.external_ip,"
        "'subscribed',CAST(EXISTS(SELECT 1 FROM pulse_subscriptions s WHERE s.device_id=d.id) AS UNSIGNED)) "
        "FROM devices d WHERE d.id=?", {id});
    statement.execute();
    auto rows = statement.rows();
    if (rows.empty()) return std::nullopt;
    auto device = rows[0];
    device["subscribed"] = device["subscribed"].get<int>() != 0;
    return device;
}
bool DeviceStore::create(const json& device) const {
    Connection connection(config_);
    Transaction transaction(connection);
    Statement insert(connection, "INSERT INTO devices (id, local_ip, external_ip) VALUES (?, ?, ?)",
        {device["id"].get<std::string>(), device["local_ip"].get<std::string>(), external(device)});
    if (!insert.execute(true)) return false;
    event(connection, "created", device);
    transaction.commit();
    return true;
}
std::optional<json> DeviceStore::patch(const std::string& id, const json& fields) const {
    Connection connection(config_);
    Transaction transaction(connection);
    auto device = lock_device(connection, id);
    if (!device) return std::nullopt;
    for (const auto* field : {"local_ip", "external_ip"})
        if (fields.contains(field)) (*device)[field] = fields[field];
    Statement update(connection, "UPDATE devices SET local_ip = ?, external_ip = ?, updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
        {(*device)["local_ip"].get<std::string>(), external(*device), id});
    update.execute();
    event(connection, "updated", *device);
    transaction.commit();
    return device;
}
bool DeviceStore::remove(const std::string& id) const {
    Connection connection(config_);
    Transaction transaction(connection);
    const auto device = lock_device(connection, id);
    if (!device) return false;
    Statement remove(connection, "DELETE FROM devices WHERE id = ?", {id});
    remove.execute();
    event(connection, "deleted", *device);
    transaction.commit();
    return true;
}
}

namespace spark {
namespace {
// Database time is authoritative; all SQL sessions use UTC.
const std::string timer_projection =
    "JSON_OBJECT('id',id,'label',label,'duration_seconds',duration_seconds,'status',status,"
    "'source_device',source_device,'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'ends_at',DATE_FORMAT(ends_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'remaining_seconds',IF(status='running',GREATEST(0,CEIL(TIMESTAMPDIFF(MICROSECOND,CURRENT_TIMESTAMP(6),ends_at)/1000000)),0))";
std::optional<json> read_timer(Connection& connection, const std::string& id, bool lock = false) {
    Statement query(connection, "SELECT " + timer_projection + " FROM timers WHERE id=?" + (lock ? " FOR UPDATE" : ""), {id});
    query.execute();
    auto rows = query.rows();
    if (rows.empty()) return std::nullopt;
    return rows[0];
}
std::optional<std::string> nullable_string(const json& value) {
    return value.is_null() ? std::nullopt : std::optional<std::string>(value.get<std::string>());
}
// Reusable transaction-bound domain event writer for timers and future services.
void append_event(Connection& connection, const std::string& type, const std::string& resource,
                  const json& source, const json& payload) {
    Statement insert(connection, "INSERT INTO events (type,resource,source_device,payload) VALUES (?,?,?,?)",
                     {type, resource, nullable_string(source), payload.dump()});
    insert.execute();
}
void timer_event(Connection& connection, const std::string& type, const json& timer, const json& source) {
    append_event(connection, type, "/timers/" + timer["id"].get<std::string>(), source, timer);
}
}
void TimerStore::verify() const {
    Connection connection(config_);
    {
        Statement query(connection, "SELECT " + timer_projection + " FROM timers LIMIT 0");
        query.execute();
        query.rows();
    }
    Statement query(connection, "SELECT event_id,type,resource,source_device,payload,recorded_at FROM events LIMIT 0");
    query.execute();
}
json TimerStore::create(const json& packet) const {
    Connection connection(config_);
    Transaction transaction(connection);
    Statement uuid(connection, "SELECT JSON_QUOTE(UUID())");
    uuid.execute();
    const std::string id = uuid.rows()[0].get<std::string>();
    const auto duration = std::to_string(packet["duration_seconds"].get<int>());
    Statement insert(connection,
        "INSERT INTO timers (id,label,duration_seconds,source_device,ends_at) VALUES (?,?,?,?,TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP(6)))",
        {id, packet.value("label", std::string()), duration, nullable_string(packet.value("source_device", json(nullptr))), duration});
    insert.execute();
    const auto timer = *read_timer(connection, id);
    timer_event(connection, "timer.created", timer, timer["source_device"]);
    transaction.commit();
    return timer;
}
void TimerStore::expire() const {
    Connection connection(config_);
    // Bounded work per call; subsequent ticks drain any backlog.
    Transaction transaction(connection);
    Statement due(connection, "SELECT JSON_QUOTE(id) FROM timers WHERE status='running' AND ends_at<=CURRENT_TIMESTAMP(6) ORDER BY ends_at,id LIMIT 100 FOR UPDATE");
    due.execute();
    for (const auto& item : due.rows()) {
        const std::string id = item.get<std::string>();
        Statement update(connection, "UPDATE timers SET status='expired',updated_at=CURRENT_TIMESTAMP(6) WHERE id=?", {id});
        update.execute();
        timer_event(connection, "timer.expired", *read_timer(connection, id), nullptr);
    }
    transaction.commit();
}
json TimerStore::active() const {
    expire();
    Connection connection(config_);
    Statement query(connection, "SELECT " + timer_projection + " FROM timers WHERE status IN ('running','expired') ORDER BY ends_at,id");
    query.execute();
    return {{"timers", query.rows()}};
}
std::optional<json> TimerStore::get(const std::string& id) const {
    expire();
    Connection connection(config_);
    return read_timer(connection, id);
}
std::optional<json> TimerStore::transition(const std::string& id, const std::string& action, const json& source) const {
    Connection connection(config_);
    Transaction transaction(connection);
    auto timer = read_timer(connection, id, true);
    if (!timer) return std::nullopt;
    // Settle this timer's deadline while holding its row lock, even between ticks.
    Statement overdue(connection, "SELECT JSON_ARRAY(status='running' AND ends_at<=CURRENT_TIMESTAMP(6)) FROM timers WHERE id=?", {id});
    overdue.execute();
    if (overdue.rows()[0][0] == 1) {
        Statement update(connection, "UPDATE timers SET status='expired',updated_at=CURRENT_TIMESTAMP(6) WHERE id=?", {id});
        update.execute();
        timer = read_timer(connection, id);
        timer_event(connection, "timer.expired", *timer, nullptr);
    }
    const std::string target = action == "confirm" ? "confirmed" : "stopped";
    if ((*timer)["status"] == target) {
        transaction.commit();
        return timer;
    }
    if ((action == "confirm" && (*timer)["status"] != "expired") ||
        (action == "stop" && (*timer)["status"] != "running" && (*timer)["status"] != "expired"))
        throw TimerConflict("invalid_timer_state");
    Statement update(connection, "UPDATE timers SET status=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?", {target, id});
    update.execute();
    timer = read_timer(connection, id);
    timer_event(connection, "timer." + target, *timer, source);
    transaction.commit();
    return timer;
}
}

namespace spark {
void NotificationStore::verify() const {
    pulse_subscribers();
    Connection connection(config_);
    Statement query(connection, "SELECT JSON_ARRAY(sequence,event_id) FROM notification_log LIMIT 0");
    query.execute();
    query.rows();
    Statement lock(connection, "SELECT JSON_ARRAY(id) FROM notification_dispatch_lock WHERE id=1");
    lock.execute();
    if (lock.rows().empty()) throw DatabaseError("Notification dispatch lock missing; run setup-db.sh");
}
void NotificationStore::sequence_events() const {
    Connection connection(config_);
    Transaction transaction(connection);
    Statement lock(connection, "SELECT JSON_ARRAY(id) FROM notification_dispatch_lock WHERE id=1 FOR UPDATE");
    lock.execute();
    if (lock.rows().empty()) throw DatabaseError("Notification dispatch lock missing");
    // A consistent read sees only committed events, including late commits with
    // smaller IDs. The separate dispatch sequence is assigned under the lock.
    Statement pending(connection, "SELECT JSON_QUOTE(CAST(e.event_id AS CHAR)) FROM events e LEFT JOIN notification_log n ON n.event_id=e.event_id WHERE n.event_id IS NULL ORDER BY e.event_id LIMIT 256");
    pending.execute();
    for (const auto& id : pending.rows()) {
        Statement insert(connection, "INSERT INTO notification_log(event_id) VALUES (?)", {id.get<std::string>()});
        insert.execute();
    }
    transaction.commit();
}
unsigned long long NotificationStore::latest() const {
    Connection connection(config_);
    Statement query(connection, "SELECT JSON_QUOTE(CAST(COALESCE(MAX(sequence),0) AS CHAR)) FROM notification_log");
    query.execute();
    return std::stoull(query.rows()[0].get<std::string>());
}
json NotificationStore::after(unsigned long long sequence) const {
    Connection connection(config_);
    Statement query(connection,
        "SELECT JSON_MERGE_PATCH(JSON_OBJECT('sequence',n.sequence,'event_id',e.event_id,'type',e.type,'resource',e.resource,'source_device',e.source_device), IF(e.type='calendar.reminder',e.payload,JSON_OBJECT())) "
        "FROM notification_log n JOIN events e ON e.event_id=n.event_id WHERE n.sequence>? ORDER BY n.sequence LIMIT 256", {std::to_string(sequence)});
    query.execute();
    return query.rows();
}
}

namespace spark {
namespace {
json read_pulse(Connection& connection, bool lock = false) {
    Statement query(connection,
        std::string("SELECT JSON_OBJECT('enabled',enabled,"
        "'interval_seconds',interval_seconds,'next_pulse_at',DATE_FORMAT(next_pulse_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
        "'last_pulse_at',DATE_FORMAT(last_pulse_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'pulse_count',pulse_count,"
        "'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ')) FROM pulse WHERE id=1") + (lock ? " FOR UPDATE" : ""));
    query.execute();
    auto rows = query.rows();
    if (rows.empty()) throw DatabaseError("Pulse settings missing; run setup-db.sh");
    // MariaDB JSON boolean handling varies with expression coercion; expose a JSON bool.
    auto record = rows[0];
    record["enabled"] = record["enabled"] == true || record["enabled"] == 1 || record["enabled"] == "true";
    return record;
}
}
void PulseStore::verify() const { get(); }
json PulseStore::get() const {
    Connection connection(config_);
    return read_pulse(connection);
}
json PulseStore::patch(const json& packet) const {
    Connection connection(config_);
    Transaction transaction(connection);
    auto previous = read_pulse(connection, true);
    const bool enabled = packet.value("enabled", previous["enabled"].get<bool>());
    const int interval = packet.value("interval_seconds", previous["interval_seconds"].get<int>());
    if (enabled == previous["enabled"].get<bool>() && interval == previous["interval_seconds"].get<int>()) {
        transaction.commit();
        return previous;
    }
    Statement update(connection,
        "UPDATE pulse SET enabled=?,interval_seconds=?,next_pulse_at=IF(?=1,TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP(6)),NULL),updated_at=CURRENT_TIMESTAMP(6) WHERE id=1",
        {enabled ? "1" : "0", std::to_string(interval), enabled ? "1" : "0", std::to_string(interval)});
    update.execute();
    auto state = read_pulse(connection);
    append_event(connection, "pulse.updated", "/pulse", packet.value("source_device", json(nullptr)), state);
    transaction.commit();
    return state;
}
void PulseStore::tick() const {
    Connection connection(config_);
    Transaction transaction(connection);
    read_pulse(connection, true);
    Statement due(connection, "SELECT JSON_ARRAY(CAST(enabled AND next_pulse_at<=CURRENT_TIMESTAMP(6) AS UNSIGNED)) FROM pulse WHERE id=1");
    due.execute();
    if (due.rows()[0][0] != 1) return;
    // Coalesce missed intervals into one pulse, then schedule from now. No catch-up burst.
    Statement update(connection,
        "UPDATE pulse SET pulse_count=pulse_count+1,last_pulse_at=CURRENT_TIMESTAMP(6),next_pulse_at=TIMESTAMPADD(SECOND,interval_seconds,CURRENT_TIMESTAMP(6)),updated_at=CURRENT_TIMESTAMP(6) WHERE id=1");
    update.execute();
    append_event(connection, "pulse.triggered", "/pulse", nullptr, read_pulse(connection));
    transaction.commit();
}
}

namespace spark {
void PulseStore::subscribe(const std::string& device_id) const {
    Connection connection(config_);
    Statement insert(connection, "INSERT INTO pulse_subscriptions(device_id) VALUES (?)", {device_id});
    insert.execute(true); // Duplicate subscriptions are successful no-ops.
}
void PulseStore::unsubscribe(const std::string& device_id) const {
    Connection connection(config_);
    Statement remove(connection, "DELETE FROM pulse_subscriptions WHERE device_id=?", {device_id});
    remove.execute();
}
std::unordered_set<std::string> NotificationStore::pulse_subscribers() const {
    Connection connection(config_);
    Statement query(connection, "SELECT JSON_QUOTE(device_id) FROM pulse_subscriptions");
    query.execute();
    std::unordered_set<std::string> subscribers;
    for (const auto& id : query.rows()) subscribers.insert(id.get<std::string>());
    return subscribers;
}
}

namespace spark {
namespace {
const std::string calendar_projection =
    "JSON_OBJECT('id',id,'name',name,'description',description,'place',place,"
    "'recurrence',JSON_EXTRACT(recurrence,'$'),'reminder_minutes',reminder_minutes,"
    "'last_reminder_occurs_at',DATE_FORMAT(last_reminder_occurs_at,'%Y-%m-%dT%H:%i:%sZ'),"
    "'occurs_at',DATE_FORMAT(occurs_at,'%Y-%m-%dT%H:%i:%sZ'),"
    "'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
std::optional<json> read_calendar(Connection& connection, const std::string& id, bool lock = false) {
    Statement query(connection, "SELECT " + calendar_projection + " FROM calendar_events WHERE id=?" + (lock ? " FOR UPDATE" : ""), {id});
    query.execute();
    auto rows = query.rows();
    return rows.empty() ? std::nullopt : std::optional<json>(rows[0]);
}
std::string sql_datetime(std::string value) { value[10] = ' '; value.pop_back(); return value; }
}
void CalendarStore::verify() const {
    Connection connection(config_);
    Statement query(connection, "SELECT " + calendar_projection + " FROM calendar_events LIMIT 0");
    query.execute(); query.rows();
}
json CalendarStore::create(const json& packet) const {
    if (!calendar_occurrence(packet,calendar_instant(packet["occurs_at"]))) throw std::invalid_argument("invalid_recurrence");
    Connection connection(config_);
    Transaction transaction(connection);
    Statement uuid(connection, "SELECT JSON_QUOTE(UUID())");
    uuid.execute();
    const auto id = uuid.rows()[0].get<std::string>();
    Statement insert(connection, "INSERT INTO calendar_events(id,name,description,occurs_at,place,recurrence,reminder_minutes) VALUES (?,?,?,?,?,?,?)",
        {id, packet["name"].get<std::string>(), packet["description"].get<std::string>(), sql_datetime(packet["occurs_at"]), nullable_string(packet.value("place", json(nullptr))), packet.value("recurrence",json(nullptr)).is_null()?std::nullopt:std::optional<std::string>(packet["recurrence"].dump()), packet.value("reminder_minutes",json(nullptr)).is_null()?std::nullopt:std::optional<std::string>(packet["reminder_minutes"].dump())});
    insert.execute();
    auto record = *read_calendar(connection, id);
    append_event(connection, "calendar.created", "/events/" + id, packet.value("source_device", json(nullptr)), record);
    transaction.commit();
    return record;
}
std::optional<json> CalendarStore::get(const std::string& id) const {
    Connection connection(config_);
    return read_calendar(connection, id);
}
std::optional<json> CalendarStore::patch(const std::string& id, const json& packet) const {
    Connection connection(config_);
    Transaction transaction(connection);
    auto record = read_calendar(connection, id, true);
    if (!record) return std::nullopt;
    auto updated = *record;
    for (const auto* field : {"name", "description", "occurs_at", "place", "recurrence", "reminder_minutes"}) if (packet.contains(field)) updated[field] = packet[field];
    if (updated == *record) { transaction.commit(); return record; }
    if (!calendar_occurrence(updated,calendar_instant(updated["occurs_at"]))) throw std::invalid_argument("invalid_recurrence");
    if(updated["occurs_at"]!=(*record)["occurs_at"] || updated["recurrence"]!=(*record)["recurrence"]) {
        Statement reset(connection,"UPDATE calendar_events SET last_reminder_occurs_at=NULL WHERE id=?",{id}); reset.execute();
    }
    Statement update(connection, "UPDATE calendar_events SET name=?,description=?,occurs_at=?,place=?,recurrence=?,reminder_minutes=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",
        {updated["name"].get<std::string>(), updated["description"].get<std::string>(), sql_datetime(updated["occurs_at"]), nullable_string(updated["place"]), updated["recurrence"].is_null()?std::nullopt:std::optional<std::string>(updated["recurrence"].dump()), updated["reminder_minutes"].is_null()?std::nullopt:std::optional<std::string>(updated["reminder_minutes"].dump()), id});
    update.execute();
    record = read_calendar(connection, id);
    append_event(connection, "calendar.updated", "/events/" + id, packet.value("source_device", json(nullptr)), *record);
    transaction.commit();
    return record;
}
bool CalendarStore::remove(const std::string& id) const {
    Connection connection(config_);
    Transaction transaction(connection);
    auto record = read_calendar(connection, id, true);
    if (!record) return false;
    Statement remove(connection, "DELETE FROM calendar_events WHERE id=?", {id});
    remove.execute();
    append_event(connection, "calendar.deleted", "/events/" + id, nullptr, *record);
    transaction.commit();
    return true;
}
void CalendarStore::tick() const {
    Connection connection(config_);
    Statement ids(connection,"SELECT JSON_QUOTE(id) FROM calendar_events WHERE reminder_minutes IS NOT NULL ORDER BY id");
    ids.execute(); const auto records=ids.rows();
    for(const auto& id:records) {
        Transaction transaction(connection);
        auto record=read_calendar(connection,id.get<std::string>(),true);
        if(!record || (*record)["reminder_minutes"].is_null()) { transaction.commit(); continue; }
        Statement clock(connection,"SELECT JSON_QUOTE(DATE_FORMAT(UTC_TIMESTAMP(),'%Y-%m-%dT%H:%i:%sZ'))");
        clock.execute(); const auto now=calendar_instant(clock.rows()[0]);
        auto from=now-60;
        if(!(*record)["last_reminder_occurs_at"].is_null()) from=std::max(from,calendar_instant((*record)["last_reminder_occurs_at"])+1);
        auto occurrence=calendar_occurrence(*record,from);
        if(occurrence && *occurrence<=now+(*record)["reminder_minutes"].get<int>()*60) {
            const auto when=calendar_timestamp(*occurrence);
            Statement mark(connection,"UPDATE calendar_events SET last_reminder_occurs_at=? WHERE id=?",{sql_datetime(when),id.get<std::string>()}); mark.execute();
            append_event(connection,"calendar.reminder","/events/"+id.get<std::string>(),nullptr,{{"occurrence_at",when}});
        }
        transaction.commit();
    }
}
json CalendarStore::list(const json& filters) const {
    Connection connection(config_);
    std::string sql = "SELECT " + calendar_projection + " FROM calendar_events WHERE 1=1";
    Parameters values;
    if (filters.contains("place")) { sql += " AND BINARY place=BINARY ?"; values.push_back(filters["place"].get<std::string>()); }
    if (filters.contains("name")) { sql += " AND LOCATE(BINARY ?,BINARY name)>0"; values.push_back(filters["name"].get<std::string>()); }
    const int limit = filters.value("limit", 100), offset = filters.value("offset", 0);
    sql += " ORDER BY occurs_at,id";
    Statement query(connection, sql, values);
    query.execute();
    auto candidates = query.rows();
    json rows=json::array();
    auto lower=calendar_instant("1000-01-01T00:00:00Z"), upper=calendar_instant("9999-12-31T23:59:59Z")+1;
    if(filters.contains("from")) lower=calendar_instant(filters["from"]);
    if(filters.contains("to")) upper=calendar_instant(filters["to"]);
    if(filters.contains("day")) {
        const auto day=calendar_instant(filters["day"].get<std::string>()+"T00:00:00Z");
        lower=std::max(lower,day); upper=std::min(upper,day+86400);
    }
    int matched=0;
    for(auto& record:candidates) {
        const auto occurrence=calendar_occurrence(record,lower);
        if(!occurrence || *occurrence>=upper) continue;
        if(matched++<offset) continue;
        rows.push_back(record);
        if(rows.size()>static_cast<std::size_t>(limit)) break;
    }
    const bool more=rows.size()>static_cast<std::size_t>(limit);
    if(more) rows.erase(rows.end()-1);
    return {{"events", rows}, {"limit", limit}, {"offset", offset}, {"has_more", more}};
}
}

namespace spark {
namespace {
const std::string daily_task_projection =
    "JSON_OBJECT('id',id,'name',name,'description',description,'checked',checked,"
    "'checked_at',DATE_FORMAT(checked_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
json task_record(json record, const std::string& day) {
    record["checked"] = record["checked"].get<int>() != 0;
    record["day"] = day;
    return record;
}
std::string settle_task_day(Connection& connection) {
    Statement lock(connection,"SELECT JSON_QUOTE(CAST(local_day AS CHAR)) FROM daily_task_day WHERE id=1 FOR UPDATE");
    lock.execute(); auto previous=lock.rows();
    if(previous.empty()) throw DatabaseError("Daily task day missing; run setup-db.sh");
    // Read the clock after obtaining the lock, so waiting across midnight is safe.
    Statement clock(connection,"SELECT JSON_QUOTE(CAST(DATE(UTC_TIMESTAMP()+INTERVAL 8 HOUR) AS CHAR))");
    clock.execute(); const auto day=clock.rows()[0].get<std::string>();
    if(previous[0]!=day) {
        Statement reset(connection,"UPDATE daily_tasks SET checked=0,checked_at=NULL,updated_at=CURRENT_TIMESTAMP(6) WHERE checked=1");
        reset.execute();
        Statement advance(connection,"UPDATE daily_task_day SET local_day=? WHERE id=1",{day}); advance.execute();
        append_event(connection,"daily_tasks.reset","/daily-tasks",nullptr,{{"day",day},{"timezone","+08:00"}});
    }
    return day;
}
std::optional<json> read_daily_task(Connection& connection,const std::string& id,const std::string& day) {
    Statement query(connection,"SELECT "+daily_task_projection+" FROM daily_tasks WHERE id=?",{id});
    query.execute(); auto rows=query.rows();
    if(rows.empty()) return std::nullopt;
    return task_record(rows[0],day);
}
void task_event(Connection& connection,const char* action,const json& task,const json& source) {
    append_event(connection,"daily_tasks.updated","/daily-tasks",source,{{"action",action},{"task",task}});
}
}
void DailyTaskStore::verify() const {
    Connection connection(config_);
    Statement tasks(connection,"SELECT "+daily_task_projection+" FROM daily_tasks LIMIT 0"); tasks.execute(); tasks.rows();
    Statement day(connection,"SELECT JSON_ARRAY(local_day) FROM daily_task_day WHERE id=1"); day.execute();
    if(day.rows().empty()) throw DatabaseError("Daily task day missing; run setup-db.sh");
}
void DailyTaskStore::tick() const { Connection c(config_); Transaction tx(c); settle_task_day(c); tx.commit(); }
json DailyTaskStore::list() const {
    Connection c(config_); Transaction tx(c); const auto day=settle_task_day(c);
    Statement query(c,"SELECT "+daily_task_projection+" FROM daily_tasks ORDER BY created_at,id"); query.execute();
    auto records=query.rows(); for(auto& task:records) task=task_record(task,day);
    tx.commit(); return {{"day",day},{"timezone","+08:00"},{"tasks",records}};
}
json DailyTaskStore::create(const json& packet) const {
    Connection c(config_); Transaction tx(c); const auto day=settle_task_day(c);
    Statement uuid(c,"SELECT JSON_QUOTE(UUID())"); uuid.execute(); const auto id=uuid.rows()[0].get<std::string>();
    Statement insert(c,"INSERT INTO daily_tasks(id,name,description) VALUES (?,?,?)",{id,packet["name"].get<std::string>(),packet.value("description",std::string())}); insert.execute();
    const auto task=*read_daily_task(c,id,day); task_event(c,"created",task,packet.value("source_device",json(nullptr))); tx.commit(); return task;
}
std::optional<json> DailyTaskStore::get(const std::string& id) const {
    Connection c(config_); Transaction tx(c); const auto day=settle_task_day(c);
    auto task=read_daily_task(c,id,day); tx.commit(); return task;
}
std::optional<json> DailyTaskStore::update(const std::string& id,const json& packet,bool check_state) const {
    Connection c(config_); Transaction tx(c); const auto day=settle_task_day(c);
    auto task=read_daily_task(c,id,day); if(!task) { tx.commit(); return std::nullopt; }
    auto desired=*task;
    if(check_state) desired["checked"]=packet["checked"];
    else for(const auto* field:{"name","description"}) if(packet.contains(field)) desired[field]=packet[field];
    if(desired==*task) { tx.commit(); return task; }
    if(check_state) {
        const bool checked=desired["checked"].get<bool>();
        Statement update(c,"UPDATE daily_tasks SET checked=?,checked_at=IF(?=1,CURRENT_TIMESTAMP(6),NULL),updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{checked?"1":"0",checked?"1":"0",id}); update.execute();
    } else {
        Statement update(c,"UPDATE daily_tasks SET name=?,description=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{desired["name"].get<std::string>(),desired["description"].get<std::string>(),id}); update.execute();
    }
    task=read_daily_task(c,id,day); task_event(c,check_state?"checked_state_changed":"modified",*task,packet.value("source_device",json(nullptr))); tx.commit(); return task;
}
bool DailyTaskStore::remove(const std::string& id) const {
    Connection c(config_); Transaction tx(c); const auto day=settle_task_day(c);
    auto task=read_daily_task(c,id,day); if(!task) { tx.commit(); return false; }
    Statement remove(c,"DELETE FROM daily_tasks WHERE id=?",{id}); remove.execute(); task_event(c,"deleted",*task,nullptr); tx.commit(); return true;
}
}

namespace spark {
namespace {
const std::string file_projection = "JSON_OBJECT('id',id,'name',name,'directory',directory,'size_bytes',size_bytes,'content_type','application/octet-stream','download_url',CONCAT('/files/',id,'/content'),'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
}
void FileStore::verify() const {
    Connection c(config_);
    { Statement q(c,"SELECT "+file_projection+",state FROM files LIMIT 0"); q.execute(); }
    Statement dirs(c,"SELECT path FROM file_directories LIMIT 0");dirs.execute();
}
namespace {
void ensure_directories(Connection& c,const std::string& directory){
    Statement root(c,"INSERT IGNORE INTO file_directories(path) VALUES ('/')");root.execute();
    for(std::size_t end=1;end<=directory.size();++end)if(end==directory.size() || directory[end]=='/'){
        if(end==1)continue;
        Statement q(c,"INSERT IGNORE INTO file_directories(path) VALUES (?)",{directory.substr(0,end)});q.execute();
    }
}
}
void FileStore::create_directory(const std::string& directory) const {
    Connection c(config_);Transaction tx(c);ensure_directories(c,directory);tx.commit();
}
json FileStore::directories() const {
    Connection c(config_);Statement q(c,"SELECT JSON_QUOTE(CONVERT(path USING utf8mb4)) FROM file_directories ORDER BY path");q.execute();return {{"directories",q.rows()}};
}
std::optional<json> FileStore::move(const std::string& id,const std::string& directory) const {
    Connection c(config_);Transaction tx(c);
    Statement read(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready' FOR UPDATE",{id});read.execute();auto rows=read.rows();if(rows.empty())return std::nullopt;
    if(rows[0]["directory"]!=directory){
        ensure_directories(c,directory);
        Statement q(c,"UPDATE files SET directory=? WHERE id=?",{directory,id});q.execute();rows[0]["directory"]=directory;
    }
    tx.commit();return rows[0];
}
std::string FileStore::begin(const std::string& name,const std::string& directory) const {
    Connection c(config_); Statement uuid(c,"SELECT JSON_QUOTE(UUID())"); uuid.execute(); auto id=uuid.rows()[0].get<std::string>();
    Transaction tx(c);ensure_directories(c,directory);Statement q(c,"INSERT INTO files(id,name,directory) VALUES (?,?,?)",{id,name,directory}); q.execute();tx.commit(); return id;
}
json FileStore::publish(const std::string& id,std::uint64_t size) const {
    Connection c(config_); Transaction tx(c);
    Statement q(c,"UPDATE files SET size_bytes=?,state='ready' WHERE id=? AND state='uploading'",{std::to_string(size),id}); q.execute();
    Statement read(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready'",{id}); read.execute(); auto rows=read.rows();
    if(rows.empty()) throw DatabaseError("File publication failed");
    tx.commit(); return rows[0];
}
std::optional<json> FileStore::get(const std::string& id) const {
    Connection c(config_); Statement q(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready'",{id});q.execute();auto rows=q.rows();
    return rows.empty()?std::nullopt:std::optional<json>(rows[0]);
}
json FileStore::list(int limit,int offset,const std::optional<std::string>& directory) const {
    Connection c(config_); Statement q(c,"SELECT "+file_projection+" FROM files WHERE state='ready'"+(directory?" AND BINARY directory=BINARY ?":"")+" ORDER BY created_at,id LIMIT "+std::to_string(limit+1)+" OFFSET "+std::to_string(offset),directory?Parameters{*directory}:Parameters{});q.execute();auto rows=q.rows();
    const bool more=rows.size()>static_cast<std::size_t>(limit);if(more)rows.erase(rows.end()-1);
    return {{"files",rows},{"limit",limit},{"offset",offset},{"has_more",more}};
}
bool FileStore::deleting(const std::string& id) const {
    Connection c(config_);Transaction tx(c);Statement q(c,"SELECT JSON_QUOTE(state) FROM files WHERE id=? FOR UPDATE",{id});q.execute();auto rows=q.rows();
    if(rows.empty() || rows[0]=="uploading")return false;
    Statement update(c,"UPDATE files SET state='deleting' WHERE id=?",{id});update.execute();tx.commit();return true;
}
void FileStore::forget(const std::string& id) const {
    Connection c(config_);Statement q(c,"DELETE FROM files WHERE id=? AND state!='ready'",{id});q.execute();
}
json FileStore::unfinished() const {
    Connection c(config_);Statement q(c,"SELECT JSON_QUOTE(id) FROM files WHERE state!='ready'");q.execute();return q.rows();
}
}
