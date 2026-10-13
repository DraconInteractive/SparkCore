#include <queue>
#include <unordered_map>
#include <functional>
#include "database.hpp"
#include "commands.hpp"
#include <sys/random.h>
#include <cerrno>
#include <algorithm>
#include "projects.hpp"
#include "boards.hpp"
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
    bool changed() const { return mysql_stmt_affected_rows(handle.get()) > 0; }
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
void append_event(Connection&, const std::string&, const std::string&, const json&, const json&);
void event(Connection& connection, const char* action, const json& device, bool notify=true) {
    Statement statement(connection,
        "INSERT INTO device_events (device_id, action, local_ip, external_ip) VALUES (?, ?, ?, ?)",
        {device["id"].get<std::string>(), std::string(action), device["local_ip"].get<std::string>(), external(device)});
    statement.execute();
    if(notify)append_event(connection,std::string("device.")+action,"/devices/"+device["id"].get<std::string>(),nullptr,json::object());
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
    const auto previous=*device;
    for (const auto* field : {"local_ip", "external_ip"})
        if (fields.contains(field)) (*device)[field] = fields[field];
    Statement update(connection, "UPDATE devices SET local_ip = ?, external_ip = ?, updated_at = CURRENT_TIMESTAMP(6) WHERE id = ?",
        {(*device)["local_ip"].get<std::string>(), external(*device), id});
    update.execute();
    event(connection, "updated", *device,previous!=*device);
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
json NotificationStore::after(unsigned long long sequence, unsigned long long through) const {
    Connection connection(config_);
    Statement query(connection,
        "SELECT JSON_MERGE_PATCH(JSON_OBJECT('sequence',n.sequence,'event_id',e.event_id,'type',e.type,'resource',e.resource,'source_device',e.source_device), IF(e.type='calendar.reminder' OR e.type IN ('command.available','command.updated','files.directories_updated'),e.payload,IF(e.type='device.subscription_updated',JSON_OBJECT('subscribed',JSON_EXTRACT(e.payload,'$.subscribed')),JSON_OBJECT()))) "
        "FROM notification_log n JOIN events e ON e.event_id=n.event_id WHERE n.sequence>? AND n.sequence<=? ORDER BY n.sequence LIMIT 256", {std::to_string(sequence),std::to_string(through)});
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
    Connection connection(config_);Transaction transaction(connection);
    Statement insert(connection, "INSERT INTO pulse_subscriptions(device_id) VALUES (?)", {device_id});
    if(insert.execute(true))append_event(connection,"device.subscription_updated","/devices/"+device_id,nullptr,{{"subscribed",true}});
    transaction.commit();
}
void PulseStore::unsubscribe(const std::string& device_id) const {
    Connection connection(config_);Transaction transaction(connection);
    Statement remove(connection, "DELETE FROM pulse_subscriptions WHERE device_id=?", {device_id});
    remove.execute();
    if(remove.changed())append_event(connection,"device.subscription_updated","/devices/"+device_id,nullptr,{{"subscribed",false}});
    transaction.commit();
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
json CalendarStore::list(const json& filters, bool expand) const {
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
    if (expand) {
        // Merge one occurrence stream per series; never materialize the entire range.
        struct Entry { std::time_t at; std::size_t index; };
        auto later = [&candidates](const Entry& a, const Entry& b) {
            if (a.at != b.at) return a.at > b.at;
            return candidates[a.index]["id"].get<std::string>() > candidates[b.index]["id"].get<std::string>();
        };
        std::priority_queue<Entry, std::vector<Entry>, decltype(later)> pending(later);
        for (std::size_t i=0; i<candidates.size(); ++i) {
            const auto at=calendar_occurrence(candidates[i],lower);
            if (at && *at<upper) pending.push({*at,i});
        }
        int skipped=0;
        while (!pending.empty() && rows.size()<static_cast<std::size_t>(limit)) {
            const auto entry=pending.top(); pending.pop();
            const auto& event=candidates[entry.index];
            if (skipped<offset) ++skipped;
            else rows.push_back({{"event_id",event["id"]},{"occurrence_at",calendar_timestamp(entry.at)},{"event",event}});
            const auto next=calendar_occurrence(event,entry.at+1);
            if (next && *next<upper) pending.push({*next,entry.index});
        }
        return {{"occurrences",rows},{"limit",limit},{"offset",offset},{"has_more",!pending.empty()}};
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
const std::string file_projection = "JSON_OBJECT('id',id,'name',name,'directory',directory,'size_bytes',size_bytes,'content_revision',CONVERT(content_revision USING utf8mb4),'drive_id',drive_id,'drive_letter',(SELECT letter FROM storage_drives WHERE storage_drives.id=files.drive_id),'content_type','application/octet-stream','download_url',CONCAT('/files/',id,'/content'),'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
}
std::uint64_t FileStore::database_bytes() const {
    Connection c(config_);Statement q(c,"SELECT JSON_ARRAY(COALESCE(SUM(DATA_LENGTH+INDEX_LENGTH),0)) FROM information_schema.TABLES WHERE TABLE_SCHEMA=?",{config_.database});q.execute();return q.rows()[0][0].get<std::uint64_t>();
}
void FileStore::verify() const {
    Connection c(config_);
    { Statement q(c,"SELECT "+file_projection+",state FROM files LIMIT 0"); q.execute(); }
    Statement dirs(c,"SELECT path FROM file_directories LIMIT 0");dirs.execute();
}
namespace {
void ensure_directories(Connection& c,const std::string& directory,const std::string& drive){
    bool changed=false;
    Statement root(c,"INSERT IGNORE INTO file_directories(drive_id,path) VALUES (?,'/')",{drive});root.execute();changed=root.changed();
    for(std::size_t end=1;end<=directory.size();++end)if(end==directory.size() || directory[end]=='/'){
        if(end==1)continue;
        Statement q(c,"INSERT IGNORE INTO file_directories(drive_id,path) VALUES (?,?)",{drive,directory.substr(0,end)});q.execute();changed=changed||q.changed();
    }
    if(changed)append_event(c,"files.directories_updated","/files/directories",nullptr,{{"drive_id",drive}});
}
}
std::string FileStore::default_drive(const std::string& directory) const {
    Connection c(config_);
    Statement projects(c,"SELECT DISTINCT JSON_QUOTE(drive_id) FROM projects WHERE drive_id IS NOT NULL AND (BINARY directory=BINARY ? OR LEFT(BINARY ?,OCTET_LENGTH(directory)+1)=CONCAT(BINARY directory,'/'))",{directory,directory});projects.execute();auto matches=projects.rows();
    if(matches.size()>1)throw std::invalid_argument("drive_required");if(matches.size()==1)return matches[0].get<std::string>();
    Statement q(c,"SELECT JSON_QUOTE(id) FROM storage_drives WHERE persistent=TRUE");q.execute();auto rows=q.rows();return rows.empty()?"":rows[0].get<std::string>();
}
void FileStore::create_directory(const std::string& directory,const std::optional<std::string>& drive) const {
    const auto selected=drive?*drive:default_drive(directory);
    Connection c(config_);Transaction tx(c);ensure_directories(c,directory,selected);tx.commit();
}
json FileStore::directories(const std::optional<std::string>& drive) const {
    const auto selected=drive?*drive:default_drive();
    Connection c(config_);Statement q(c,"SELECT JSON_QUOTE(CONVERT(path USING utf8mb4)) FROM file_directories WHERE drive_id=? ORDER BY path",{selected});q.execute();return {{"drive_id",selected},{"directories",q.rows()}};
}
json FileStore::change_directory(const std::string& drive,const std::string& directory,const std::optional<std::string>& destination,bool recursive) const {
    if(directory=="/")throw std::invalid_argument("directory_root_protected");
    const auto prefix=directory+"/";
    if(destination && (*destination==directory || destination->rfind(prefix,0)==0))throw std::invalid_argument("invalid_destination");
    Connection c(config_);Transaction tx(c);
    Statement all(c,"SELECT JSON_QUOTE(CONVERT(path USING utf8mb4)) FROM file_directories WHERE drive_id=? FOR UPDATE",{drive});all.execute();auto paths=all.rows();
    bool found=false;json affected=json::array();
    for(const auto& item:paths){auto path=item.get<std::string>();if(path==directory)found=true;if(path==directory||path.rfind(prefix,0)==0)affected.push_back(path);if(destination&&path==*destination)throw std::invalid_argument("directory_conflict");}
    if(!found)throw std::invalid_argument("directory_not_found");
    Statement projects(c,"SELECT JSON_QUOTE(CONVERT(directory USING utf8mb4)) FROM projects WHERE drive_id=? FOR UPDATE",{drive});projects.execute();
    for(const auto& item:projects.rows()){auto path=item.get<std::string>();if(path==directory||path.rfind(prefix,0)==0)throw std::invalid_argument("directory_in_use");}
    const std::string match="drive_id=? AND (BINARY directory=BINARY ? OR LEFT(BINARY directory,OCTET_LENGTH(?))=BINARY ?)";
    Statement read(c,"SELECT JSON_OBJECT('id',id,'state',state) FROM files WHERE "+match+" FOR UPDATE",{drive,directory,prefix,prefix});read.execute();auto files=read.rows();
    if(!destination&&!recursive&&(affected.size()>1||!files.empty()))throw std::invalid_argument("directory_not_empty");
    if(destination){
        for(const auto& item:affected){auto path=*destination+item.get<std::string>().substr(directory.size());if(path.size()>1024)throw std::invalid_argument("directory_too_long");}
        auto parent=destination->substr(0,destination->find_last_of('/'));ensure_directories(c,parent.empty()?"/":parent,drive);
        for(const auto& item:affected){auto old=item.get<std::string>();Statement update(c,"UPDATE file_directories SET path=? WHERE drive_id=? AND path=?",{*destination+old.substr(directory.size()),drive,old});update.execute();}
        Statement update(c,"UPDATE files SET directory=CONCAT(?,SUBSTRING(BINARY directory,?)) WHERE "+match,{*destination,std::to_string(directory.size()+1),drive,directory,prefix,prefix});update.execute();
    }else{
        Statement mark(c,"UPDATE files SET state='deleting' WHERE "+match,{drive,directory,prefix,prefix});mark.execute();
        for(const auto& item:affected){Statement remove(c,"DELETE FROM file_directories WHERE drive_id=? AND path=?",{drive,item.get<std::string>()});remove.execute();}
    }
    json ids=json::array();for(const auto& file:files){ids.push_back(file["id"]);if(file["state"]=="ready")append_event(c,destination?"file.updated":"file.deleted","/files/"+file["id"].get<std::string>(),nullptr,json::object());}
    append_event(c,"files.directories_updated","/files/directories",nullptr,{{"drive_id",drive}});tx.commit();return ids;
}
std::optional<json> FileStore::move(const std::string& id,const std::string& directory) const {
    Connection c(config_);Transaction tx(c);
    Statement read(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready' FOR UPDATE",{id});read.execute();auto rows=read.rows();if(rows.empty())return std::nullopt;
    if(rows[0]["directory"]!=directory){
        ensure_directories(c,directory,rows[0]["drive_id"].is_null()?"":rows[0]["drive_id"].get<std::string>());
        Statement q(c,"UPDATE files SET directory=? WHERE id=?",{directory,id});q.execute();rows[0]["directory"]=directory;
        append_event(c,"file.updated","/files/"+id,nullptr,json::object());
    }
    tx.commit();return rows[0];
}
std::string FileStore::begin(const std::string& name,const std::string& directory,const std::optional<std::string>& drive) const {
    Connection c(config_); Statement uuid(c,"SELECT JSON_QUOTE(UUID())"); uuid.execute(); auto id=uuid.rows()[0].get<std::string>();
    Transaction tx(c);ensure_directories(c,directory,drive.value_or(""));Statement q(c,"INSERT INTO files(id,name,directory,drive_id) VALUES (?,?,?,?)",{id,name,directory,drive}); q.execute();tx.commit(); return id;
}
json FileStore::publish(const std::string& id,std::uint64_t size) const {
    Connection c(config_); Transaction tx(c);
    Statement q(c,"UPDATE files SET size_bytes=?,state='ready' WHERE id=? AND state='uploading'",{std::to_string(size),id}); q.execute();const bool changed=q.changed();
    Statement read(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready'",{id}); read.execute(); auto rows=read.rows();
    if(rows.empty()) throw DatabaseError("File publication failed");
    if(changed)append_event(c,"file.created","/files/"+id,nullptr,json::object());
    tx.commit(); return rows[0];
}
json FileStore::replace(const std::string& id,const std::string& revision,std::uint64_t size) const {
    Connection c(config_);Transaction tx(c);
    Statement update(c,"UPDATE files SET content_revision=?,size_bytes=? WHERE id=? AND state='ready'",{revision,std::to_string(size),id});update.execute();const bool changed=update.changed();
    Statement read(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready'",{id});read.execute();auto rows=read.rows();
    if(rows.empty())throw DatabaseError("File replacement failed");
    if(changed)append_event(c,"file.updated","/files/"+id,nullptr,json::object());
    tx.commit();return rows[0];
}
std::optional<json> FileStore::get(const std::string& id) const {
    Connection c(config_); Statement q(c,"SELECT "+file_projection+" FROM files WHERE id=? AND state='ready'",{id});q.execute();auto rows=q.rows();
    return rows.empty()?std::nullopt:std::optional<json>(rows[0]);
}
json FileStore::list(int limit,int offset,const std::optional<std::string>& directory,const std::optional<std::string>& drive) const {
    Parameters params;if(directory)params.push_back(*directory);if(drive)params.push_back(*drive);
    Connection c(config_); Statement q(c,"SELECT "+file_projection+" FROM files WHERE state='ready'"+(directory?" AND BINARY directory=BINARY ?":"")+(drive?" AND drive_id=?":"")+" ORDER BY created_at,id LIMIT "+std::to_string(limit+1)+" OFFSET "+std::to_string(offset),params);q.execute();auto rows=q.rows();
    const bool more=rows.size()>static_cast<std::size_t>(limit);if(more)rows.erase(rows.end()-1);
    return {{"files",rows},{"limit",limit},{"offset",offset},{"has_more",more}};
}
bool FileStore::deleting(const std::string& id) const {
    Connection c(config_);Transaction tx(c);Statement q(c,"SELECT JSON_QUOTE(state) FROM files WHERE id=? FOR UPDATE",{id});q.execute();auto rows=q.rows();
    if(rows.empty() || rows[0]=="uploading")return false;
    Statement update(c,"UPDATE files SET state='deleting' WHERE id=?",{id});update.execute();
    if(rows[0]=="ready")append_event(c,"file.deleted","/files/"+id,nullptr,json::object());
    tx.commit();return true;
}
void FileStore::forget(const std::string& id) const {
    Connection c(config_);Statement q(c,"DELETE FROM files WHERE id=? AND state!='ready'",{id});q.execute();
}
json FileStore::unfinished() const {
    Connection c(config_);Statement q(c,"SELECT JSON_QUOTE(id) FROM files WHERE state!='ready'");q.execute();return q.rows();
}
}

namespace spark {
namespace {
const std::string project_projection="JSON_OBJECT('id',id,'name',CONVERT(name USING utf8mb4),'parent_id',parent_id,'description',description,'directory',CONVERT(directory USING utf8mb4),'drive_id',drive_id,'drive_letter',(SELECT letter FROM storage_drives WHERE storage_drives.id=projects.drive_id),'github_url',github_url,'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
json project_children(Connection& c,const std::string& id){
    Statement q(c,"SELECT JSON_QUOTE(id) FROM projects WHERE parent_id=? ORDER BY name,id",{id});q.execute();return q.rows();
}
std::optional<json> read_project(Connection& c,const std::string& id){
    Statement q(c,"SELECT "+project_projection+" FROM projects WHERE id=? FOR UPDATE",{id});q.execute();auto rows=q.rows();if(rows.empty())return std::nullopt;
    rows[0]["subprojects"]=project_children(c,id);return rows[0];
}
void touch_project(Connection& c,const json& parent){
    if(parent.is_null())return;
    Statement q(c,"UPDATE projects SET updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{parent.get<std::string>()});q.execute();
    if(q.changed())append_event(c,"project.updated","/projects/"+parent.get<std::string>(),nullptr,json::object());
}
void project_conflict(Connection& c,const std::string& name,const std::string& directory,const std::string& id,const json& parent,const std::string& drive){
    Statement q(c,"SELECT JSON_QUOTE(id) FROM projects WHERE drive_id=? AND ((name=? AND parent_id <=> ?) OR directory=?) AND id!=?",{drive,name,parent.is_null()?std::nullopt:std::optional<std::string>(parent.get<std::string>()),directory,id});q.execute();if(!q.rows().empty())throw std::invalid_argument("project_conflict");
}
}
void ProjectStore::verify() const {Connection c(config_);Statement q(c,"SELECT "+project_projection+" FROM projects LIMIT 0");q.execute();}
json ProjectStore::create(const json& packet) const {
    Connection c(config_);Transaction tx(c);
    const auto name=packet["name"].get<std::string>();const auto parent=packet.value("parent_id",json(nullptr));
    std::string base="/projects",drive=packet.at("drive_id").get<std::string>();
    if(!parent.is_null()){
        auto record=read_project(c,parent.get<std::string>());if(!record)throw std::invalid_argument("parent_not_found");base=(*record)["directory"].get<std::string>();if((*record)["drive_id"]!=drive)throw std::invalid_argument("parent_drive_mismatch");
    }
    const auto directory=base+"/"+name;
    if(directory.size()>1024)throw std::invalid_argument("directory_too_long");
    project_conflict(c,name,directory,"",parent,drive);
    Statement uuid(c,"SELECT JSON_QUOTE(UUID())");uuid.execute();auto id=uuid.rows()[0].get<std::string>();
    ensure_directories(c,directory,drive);
    Statement q(c,"INSERT INTO projects(drive_id,id,name,description,directory,github_url,parent_id) VALUES (?,?,?,?,?,?,?)",{drive,id,name,packet["description"].get<std::string>(),directory,packet.value("github_url",json(nullptr)).is_null()?std::nullopt:std::optional<std::string>(packet["github_url"].get<std::string>()),parent.is_null()?std::nullopt:std::optional<std::string>(parent.get<std::string>())});q.execute();
    touch_project(c,parent);auto record=read_project(c,id);append_event(c,"project.created","/projects/"+id,nullptr,json::object());tx.commit();return *record;
}
std::optional<json> ProjectStore::get(const std::string& id,bool deep) const {
    Connection c(config_);
    if(!deep){Transaction tx(c);auto record=read_project(c,id);tx.commit();return record;}
    // One SELECT supplies a consistent snapshot, with no truncation or per-node queries.
    Statement q(c,"SELECT "+project_projection+" FROM projects ORDER BY name,id");q.execute();auto rows=q.rows();
    std::unordered_map<std::string,std::size_t> positions;
    std::unordered_map<std::string,std::vector<std::string>> children;
    for(std::size_t i=0;i<rows.size();++i){auto key=rows[i]["id"].get<std::string>();positions[key]=i;if(!rows[i]["parent_id"].is_null())children[rows[i]["parent_id"].get<std::string>()].push_back(key);}
    if(!positions.count(id))return std::nullopt;
    std::unordered_map<std::string,bool> visiting;
    std::function<json(const std::string&)> expand=[&](const std::string& key){
        if(visiting[key])throw DatabaseError("Project hierarchy cycle");visiting[key]=true;
        json record=rows[positions.at(key)];record["subprojects"]=json::array();
        const auto found=children.find(key);if(found!=children.end())for(const auto& child:found->second)record["subprojects"].push_back(expand(child));
        visiting[key]=false;return record;
    };
    return expand(id);
}
json ProjectStore::list(int limit,int offset,const std::optional<std::string>& parent) const {
    Connection c(config_);Transaction tx(c);
    std::string filter=parent?(parent->empty()?" WHERE parent_id IS NULL":" WHERE parent_id=?"):"";
    Statement q(c,"SELECT "+project_projection+" FROM projects"+filter+" ORDER BY name,id LIMIT "+std::to_string(limit+1)+" OFFSET "+std::to_string(offset),parent && !parent->empty()?Parameters{*parent}:Parameters{});q.execute();auto rows=q.rows();bool more=rows.size()>static_cast<std::size_t>(limit);if(more)rows.erase(rows.end()-1);
    for(auto& row:rows)row["subprojects"]=project_children(c,row["id"].get<std::string>());
    tx.commit();return {{"projects",rows},{"limit",limit},{"offset",offset},{"has_more",more}};
}
std::optional<json> ProjectStore::patch(const std::string& id,const json& packet) const {
    Connection c(config_);Transaction tx(c);auto record=read_project(c,id);if(!record)return std::nullopt;
    auto updated=*record;for(const auto& field:packet.items())updated[field.key()]=field.value();
    project_conflict(c,updated["name"],updated["directory"],id,updated["parent_id"],updated["drive_id"]);
    if(updated!=*record){Statement q(c,"UPDATE projects SET name=?,description=?,github_url=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{updated["name"].get<std::string>(),updated["description"].get<std::string>(),updated["github_url"].is_null()?std::nullopt:std::optional<std::string>(updated["github_url"].get<std::string>()),id});q.execute();append_event(c,"project.updated","/projects/"+id,nullptr,json::object());}
    record=read_project(c,id);tx.commit();return record;
}
std::optional<json> ProjectStore::remove(const std::string& id,bool destroy) const {
    Connection c(config_);Transaction tx(c);auto record=read_project(c,id);if(!record)return std::nullopt;
    std::vector<std::string> descendants{id};
    for(std::size_t i=0;i<descendants.size();++i){
        auto child=read_project(c,descendants[i]);if(!child)throw DatabaseError("Project hierarchy changed");
        for(const auto& key:(*child)["subprojects"])descendants.push_back(key.get<std::string>());
    }
    json files=json::array();
    if(destroy){
        const auto directory=(*record)["directory"].get<std::string>(),prefix=directory+"/";
        const auto drive=(*record)["drive_id"].get<std::string>();
        const std::string match="drive_id=? AND (BINARY directory=BINARY ? OR LEFT(BINARY directory,OCTET_LENGTH(?))=BINARY ?)";
        Statement read(c,"SELECT JSON_OBJECT('id',id,'state',state) FROM files WHERE "+match+" FOR UPDATE",{drive,directory,prefix,prefix});read.execute();
        for(const auto& file:read.rows()){
            files.push_back(file["id"]);
            if(file["state"]=="ready")append_event(c,"file.deleted","/files/"+file["id"].get<std::string>(),nullptr,json::object());
        }
        Statement mark(c,"UPDATE files SET state='deleting' WHERE "+match,{drive,directory,prefix,prefix});mark.execute();
        Statement dirs(c,"DELETE FROM file_directories WHERE drive_id=? AND (path=? OR LEFT(path,OCTET_LENGTH(?))=?)",{drive,directory,prefix,prefix});dirs.execute();
        if(dirs.changed())append_event(c,"files.directories_updated","/files/directories",nullptr,{{"drive_id",drive}});
    }
    for(auto it=descendants.rbegin();it!=descendants.rend();++it){
        Statement boards(c,"SELECT JSON_QUOTE(id) FROM project_boards WHERE project_id=?",{*it});boards.execute();auto ids=boards.rows();
        for(const auto& board:ids)append_event(c,"board.deleted","/projects/"+*it+"/boards/"+board.get<std::string>(),nullptr,{{"id",board}});
        Statement q(c,"DELETE FROM projects WHERE id=?",{*it});q.execute();
        append_event(c,"project.deleted","/projects/"+*it,nullptr,json::object());
    }
    touch_project(c,(*record)["parent_id"]);tx.commit();return files;
}
}

namespace spark {
namespace {
const std::string board_projection="JSON_OBJECT('id',CONVERT(id USING utf8mb4),'project_id',CONVERT(project_id USING utf8mb4),'name',CONVERT(name USING utf8mb4),'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
const std::string column_projection="JSON_OBJECT('id',CONVERT(id USING utf8mb4),'board_id',CONVERT(board_id USING utf8mb4),'name',CONVERT(name USING utf8mb4),'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
const std::string task_projection="JSON_OBJECT('id',CONVERT(id USING utf8mb4),'board_id',CONVERT(board_id USING utf8mb4),'column_id',CONVERT(column_id USING utf8mb4),'name',name,'description',description,'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
json board_rows(Connection& c,const std::string& sql,Parameters parameters={}){Statement q(c,sql,std::move(parameters));q.execute();return q.rows();}
json required_board_row(Connection& c,const std::string& sql,Parameters parameters,const char* error){auto rows=board_rows(c,sql,std::move(parameters));if(rows.empty())throw std::invalid_argument(error);return rows[0];}
json full_board(Connection& c,json board){
    auto id=board["id"].get<std::string>();
    board["columns"]=board_rows(c,"SELECT "+column_projection+" FROM project_columns WHERE board_id=? ORDER BY created_at,id",{id});
    auto tasks=board_rows(c,"SELECT "+task_projection+" FROM project_tasks WHERE board_id=? ORDER BY created_at,id",{id});
    std::unordered_map<std::string,std::size_t> columns;
    for(std::size_t i=0;i<board["columns"].size();++i){auto& column=board["columns"][i];columns[column["id"].get<std::string>()]=i;column["tasks"]=json::array();}
    for(const auto& task:tasks)board["columns"][columns.at(task["column_id"].get<std::string>())]["tasks"].push_back(task);
    return board;
}
void board_conflict(Connection& c,const std::string& table,const std::string& owner_field,const std::string& owner,const std::string& name,const std::string& id){
    if(!board_rows(c,"SELECT JSON_QUOTE(id) FROM "+table+" WHERE "+owner_field+"=? AND name=? AND id!=?",{owner,name,id}).empty())throw std::invalid_argument("name_conflict");
}
}
void BoardStore::verify() const {
    Connection c(config_);
    for(const auto& pair:std::vector<std::pair<std::string,std::string>>{{"project_boards",board_projection},{"project_columns",column_projection},{"project_tasks",task_projection}}){Statement q(c,"SELECT "+pair.second+" FROM "+pair.first+" LIMIT 0");try{q.execute();}catch(const DatabaseError&){throw DatabaseError("Cannot verify "+pair.first+": "+mysql_stmt_error(q.handle.get()));}}
}
json BoardStore::execute(const std::string& project,const std::string& board_id,const std::string& kind,const std::string& item_id,const std::string& method,const json& packet) const {
    Connection c(config_);Transaction tx(c);
    // All operations lock the project first, then its board. This also coordinates
    // with project deletion and serializes move/delete races at the database level.
    required_board_row(c,"SELECT JSON_QUOTE(id) FROM projects WHERE id=? FOR UPDATE",{project},"project_not_found");
    const bool read=method=="GET" || method=="HEAD",create=method=="POST",remove=method=="DELETE";
    if(board_id.empty()){
        if(read){auto rows=board_rows(c,"SELECT "+board_projection+" FROM project_boards WHERE project_id=? ORDER BY created_at,id",{project});tx.commit();return {{"boards",rows}};}
        board_conflict(c,"project_boards","project_id",project,packet["name"],"");
        auto id=board_rows(c,"SELECT JSON_QUOTE(UUID())")[0].get<std::string>();
        Statement q(c,"INSERT INTO project_boards(id,project_id,name) VALUES (?,?,?)",{id,project,packet["name"].get<std::string>()});q.execute();
        auto record=required_board_row(c,"SELECT "+board_projection+" FROM project_boards WHERE id=?",{id},"board_not_found");record["columns"]=json::array();
        append_event(c,"board.created","/projects/"+project+"/boards/"+id,nullptr,record);touch_project(c,project);tx.commit();return record;
    }
    auto board=required_board_row(c,"SELECT "+board_projection+" FROM project_boards WHERE id=? AND project_id=? FOR UPDATE",{board_id,project},"board_not_found");
    if(kind=="board" && read){auto record=full_board(c,board);tx.commit();return record;}
    const auto resource="/projects/"+project+"/boards/"+board_id;
    if(kind=="board" && remove){
        Statement q(c,"DELETE FROM project_boards WHERE id=?",{board_id});q.execute();append_event(c,"board.deleted",resource,nullptr,board);touch_project(c,project);tx.commit();return nullptr;
    }
    std::string id=item_id;json record;bool changed=create||remove;
    const std::string table=kind=="column"?"project_columns":kind=="task"?"project_tasks":"project_boards";
    const std::string projection=kind=="column"?column_projection:kind=="task"?task_projection:board_projection;
    if(create){
        if(kind=="task")required_board_row(c,"SELECT JSON_QUOTE(id) FROM project_columns WHERE id=? AND board_id=?",{packet["column_id"].get<std::string>(),board_id},"column_not_found");
        else board_conflict(c,table,"board_id",board_id,packet["name"],"");
        id=board_rows(c,"SELECT JSON_QUOTE(UUID())")[0].get<std::string>();
        if(kind=="task"){Statement q(c,"INSERT INTO project_tasks(id,board_id,column_id,name,description) VALUES (?,?,?,?,?)",{id,board_id,packet["column_id"].get<std::string>(),packet["name"].get<std::string>(),packet["description"].get<std::string>()});q.execute();}
        else{Statement q(c,"INSERT INTO project_columns(id,board_id,name) VALUES (?,?,?)",{id,board_id,packet["name"].get<std::string>()});q.execute();}
    }else{
        if(kind=="board"){record=board;id=board_id;}
        else record=required_board_row(c,"SELECT "+projection+" FROM "+table+" WHERE id=? AND board_id=?",{id,board_id},kind=="task"?"task_not_found":"column_not_found");
        if(read){tx.commit();return record;}
        if(remove){
            if(kind=="column" && !board_rows(c,"SELECT JSON_QUOTE(id) FROM project_tasks WHERE column_id=? LIMIT 1",{id}).empty())throw std::invalid_argument("column_not_empty");
            Statement q(c,"DELETE FROM "+table+" WHERE id=?",{id});q.execute();
        }else{
            auto desired=record;for(const auto& field:packet.items())desired[field.key()]=field.value();changed=desired!=record;
            if(kind=="task"){
                required_board_row(c,"SELECT JSON_QUOTE(id) FROM project_columns WHERE id=? AND board_id=?",{desired["column_id"].get<std::string>(),board_id},"column_not_found");
                if(changed){Statement q(c,"UPDATE project_tasks SET name=?,description=?,column_id=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{desired["name"].get<std::string>(),desired["description"].get<std::string>(),desired["column_id"].get<std::string>(),id});q.execute();}
            }else{
                board_conflict(c,table,kind=="board"?"project_id":"board_id",kind=="board"?project:board_id,desired["name"],id);
                if(changed){Statement q(c,"UPDATE "+table+" SET name=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{desired["name"].get<std::string>(),id});q.execute();}
            }
        }
    }
    if(changed){
        Statement q(c,"UPDATE project_boards SET updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{board_id});q.execute();touch_project(c,project);
        append_event(c,"board.updated",resource,nullptr,{{"kind",kind},{"id",id},{"action",create?"created":remove?"deleted":"updated"}});
    }
    if(!remove){record=required_board_row(c,"SELECT "+projection+" FROM "+table+" WHERE id=?",{id},"board_not_found");if(kind=="board")record=full_board(c,record);}
    tx.commit();return remove?json(nullptr):record;
}
}

namespace spark {
namespace {
const std::string command_projection =
    "JSON_OBJECT('id',id,'source_device',source_device,'source_admin',source_admin,'target_device',target_device,"
    "'executing_device',executing_device,'action',action,'input',JSON_EXTRACT(input,'$'),"
    "'idempotency_key',idempotency_key,'expires_in_seconds',expires_in_seconds,'status',status,"
    "'result',JSON_EXTRACT(result,'$'),'error',JSON_EXTRACT(error,'$'),"
    "'created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'expires_at',DATE_FORMAT(expires_at,'%Y-%m-%dT%H:%i:%s.%fZ'),"
    "'lease_expires_at',DATE_FORMAT(lease_expires_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
void queue_lock(Connection& c) {
    Statement q(c,"SELECT JSON_ARRAY(id) FROM command_queue_lock WHERE id=1 FOR UPDATE");q.execute();
    if(q.rows().empty())throw DatabaseError("Command queue lock missing; run setup-db.sh");
}
json read_command(Connection& c,const std::string& id) {
    Statement q(c,"SELECT "+command_projection+" FROM commands WHERE id=?",{id});q.execute();auto rows=q.rows();
    if(rows.empty())throw CommandError(404,"command_not_found");return rows[0];
}
void command_event(Connection& c,const char* type,const json& record) {
    append_event(c,type,"/commands/"+record["id"].get<std::string>(),record["source_device"],
        {{"action",record["action"]},{"target_device",record["target_device"]},
         {"executing_device",record["executing_device"]},{"status",record["status"]}});
}
void sweep_commands(Connection& c) {
    Statement q(c,"SELECT JSON_ARRAY(id,status) FROM commands WHERE (status='queued' AND expires_at<=CURRENT_TIMESTAMP(6)) OR (status='running' AND lease_expires_at<=CURRENT_TIMESTAMP(6))");q.execute();
    for(const auto& row:q.rows()) {
        std::string id=row[0];std::string state=row[1]=="queued"?"expired":"outcome_unknown";
        Statement update(c,"UPDATE commands SET status=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{state,id});update.execute();
        command_event(c,"command.updated",read_command(c,id));
    }
}
void require_device(Connection& c,const std::string& id) {
    if(!lock_device(c,id))throw CommandError(404,"device_not_found");
}
std::string command_token() {
    unsigned char bytes[32];std::size_t offset=0;
    while(offset<sizeof(bytes)){auto n=getrandom(bytes+offset,sizeof(bytes)-offset,0);if(n<0&&errno==EINTR)continue;if(n<=0)throw DatabaseError("Cannot generate claim token");offset+=n;}
    const char* hex="0123456789abcdef";std::string token;for(auto b:bytes){token+=hex[b>>4];token+=hex[b&15];}return token;
}
}
void CommandStore::verify() const {
    Connection c(config_);Statement q(c,"SELECT "+command_projection+" FROM commands LIMIT 0");q.execute();q.rows();
    Statement actions(c,"SELECT JSON_ARRAY(device_id,action) FROM device_actions LIMIT 0");actions.execute();actions.rows();
    Transaction tx(c);queue_lock(c);tx.commit();
}
json CommandStore::actions(const std::string& device,const json& replacement) const {
    Connection c(config_);Transaction tx(c);queue_lock(c);require_device(c,device);
    Statement query(c,"SELECT JSON_QUOTE(action) FROM device_actions WHERE device_id=? ORDER BY action",{device});query.execute();auto old=query.rows();
    json current=old;
    if(!replacement.is_null()) {
        current=replacement;std::sort(current.begin(),current.end());
        if(current!=old){
            Statement remove(c,"DELETE FROM device_actions WHERE device_id=?",{device});remove.execute();
            for(const auto& action:current){Statement insert(c,"INSERT INTO device_actions(device_id,action) VALUES (?,?)",{device,action.get<std::string>()});insert.execute();}
            append_event(c,"device.actions_updated","/devices/"+device+"/actions",device,json::object());
        }
    }
    tx.commit();return {{"device_id",device},{"actions",current}};
}
json CommandStore::create(const json& p,const std::optional<std::string>& administrator) const {
    Connection c(config_);Transaction tx(c);queue_lock(c);sweep_commands(c);
    const json source=administrator?json(nullptr):p.at("source_device");const auto target=p.value("target_device",json(nullptr));
    const std::string requester=administrator?"admin:"+*administrator:"device:"+source.get<std::string>();
    Statement existing(c,"SELECT JSON_QUOTE(id) FROM commands WHERE requester_key=? AND idempotency_key=?",{requester,p["idempotency_key"].get<std::string>()});existing.execute();auto ids=existing.rows();
    if(!ids.empty()) {
        auto record=read_command(c,ids[0]);
        if(record["target_device"]!=target || record["action"]!=p["action"] || record["input"]!=p.value("input",json::object()) || record["expires_in_seconds"]!=p.value("expires_in_seconds",300))throw CommandError(409,"idempotency_conflict");
        tx.commit();return record;
    }
    if(!administrator)require_device(c,source);if(!target.is_null())require_device(c,target);
    Statement uuid(c,"SELECT JSON_QUOTE(UUID())");uuid.execute();std::string id=uuid.rows()[0];
    auto seconds=std::to_string(p.value("expires_in_seconds",300));
    Statement insert(c,"INSERT INTO commands(id,source_device,source_admin,target_device,action,input,idempotency_key,expires_in_seconds,expires_at) VALUES (?,?,?,?,?,?,?,?,TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP(6)))",{id,nullable_string(source),administrator?std::optional<std::string>(*administrator):std::nullopt,nullable_string(target),p["action"].get<std::string>(),p.value("input",json::object()).dump(),p["idempotency_key"].get<std::string>(),seconds,seconds});insert.execute();
    auto record=read_command(c,id);command_event(c,"command.available",record);command_event(c,"command.updated",record);tx.commit();return record;
}
json CommandStore::get(const std::string& id) const {
    Connection c(config_);Transaction tx(c);queue_lock(c);sweep_commands(c);auto record=read_command(c,id);tx.commit();return record;
}
json CommandStore::list(const json& filters) const {
    Connection c(config_);Transaction tx(c);queue_lock(c);sweep_commands(c);
    std::string sql="SELECT "+command_projection+" FROM commands WHERE 1=1";Parameters args;
    for(const auto* field:{"source_device","target_device","executing_device","status"})if(filters.contains(field)){sql+=" AND "+std::string(field)+"=?";args.push_back(filters[field].get<std::string>());}
    sql+=" ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?";args.push_back(std::to_string(filters.value("limit",100)));args.push_back(std::to_string(filters.value("offset",0)));
    Statement q(c,sql,args);q.execute();auto records=q.rows();tx.commit();return {{"commands",records},{"limit",filters.value("limit",100)},{"offset",filters.value("offset",0)}};
}
json CommandStore::claim(const std::string& device,int lease_seconds,const std::optional<std::string>& command_id) const {
    Connection c(config_);Transaction tx(c);queue_lock(c);sweep_commands(c);require_device(c,device);
    std::string selection="SELECT JSON_QUOTE(c.id) FROM commands c JOIN device_actions a ON a.action=c.action AND a.device_id=? WHERE c.status='queued' AND (c.target_device IS NULL OR c.target_device=?)";
    Parameters parameters{device,device};
    if(command_id){selection+=" AND c.id=?";parameters.push_back(*command_id);}
    selection+=" ORDER BY c.created_at,c.id LIMIT 1";
    Statement q(c,selection,parameters);q.execute();auto ids=q.rows();
    if(ids.empty()){tx.commit();return {{"command",nullptr}};}
    std::string id=ids[0];const auto token=command_token();
    Statement update(c,"UPDATE commands SET status='running',executing_device=?,claim_token=?,lease_expires_at=TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP(6)),updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{device,token,std::to_string(lease_seconds),id});update.execute();
    auto record=read_command(c,id);command_event(c,"command.updated",record);tx.commit();record["claim_token"]=token;return {{"command",record}};
}
json CommandStore::transition(const std::string& id,const std::string& operation,const json& p,const std::optional<std::string>& administrator) const {
    Connection c(config_);Transaction tx(c);queue_lock(c);sweep_commands(c);auto record=read_command(c,id);
    if(operation=="cancel") {
        if(administrator ? record["source_admin"]!=*administrator : (!record["source_admin"].is_null()||record["source_device"]!=p["source_device"]))throw CommandError(403,"source_device_mismatch");
        if(record["status"]=="cancelled"){tx.commit();return record;}
        if(record["status"]!="queued")throw CommandError(409,"invalid_command_state");
        Statement update(c,"UPDATE commands SET status='cancelled',updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{id});update.execute();
    } else {
        Statement token(c,"SELECT JSON_QUOTE(claim_token) FROM commands WHERE id=?",{id});token.execute();
        if(record["executing_device"]!=p["device_id"] || token.rows()[0]!=p["claim_token"])throw CommandError(403,"invalid_claim");
        if(operation=="complete" && (record["status"]=="succeeded" || record["status"]=="failed")) {
            if(record["status"]!=p["status"] || record["result"]!=p.value("result",json(nullptr)) || record["error"]!=p.value("error",json(nullptr)))throw CommandError(409,"completion_conflict");
            tx.commit();return record;
        }
        if(record["status"]!="running")throw CommandError(409,"invalid_command_state");
        if(operation=="renew") {
            Statement update(c,"UPDATE commands SET lease_expires_at=TIMESTAMPADD(SECOND,?,CURRENT_TIMESTAMP(6)),updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{std::to_string(p.value("lease_seconds",60)),id});update.execute();
        } else {
            Statement update(c,"UPDATE commands SET status=?,result=?,error=?,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{p["status"].get<std::string>(),p.value("result",json(nullptr)).dump(),p.value("error",json(nullptr)).dump(),id});update.execute();
        }
    }
    record=read_command(c,id);command_event(c,"command.updated",record);tx.commit();return record;
}
void CommandStore::expire() const {
    Connection c(config_);Transaction tx(c);queue_lock(c);sweep_commands(c);tx.commit();
}
std::unordered_set<std::string> NotificationStore::device_actions(const std::string& device) const {
    Connection c(config_);Statement q(c,"SELECT JSON_QUOTE(action) FROM device_actions WHERE device_id=?",{device});q.execute();std::unordered_set<std::string> result;for(const auto& row:q.rows())result.insert(row.get<std::string>());return result;
}
}

namespace spark {
json CommandStore::providers(int limit,int offset) const {
    Connection c(config_);
    Statement q(c,"SELECT JSON_OBJECT('id',d.id,'local_ip',d.local_ip,'external_ip',d.external_ip,'actions',JSON_ARRAYAGG(a.action)) FROM (SELECT id,local_ip,external_ip FROM devices WHERE EXISTS(SELECT 1 FROM device_actions WHERE device_id=devices.id) ORDER BY id LIMIT ? OFFSET ?) d JOIN device_actions a ON a.device_id=d.id GROUP BY d.id,d.local_ip,d.external_ip ORDER BY d.id",{std::to_string(limit),std::to_string(offset)});
    q.execute();auto devices=q.rows();for(auto& device:devices)std::sort(device["actions"].begin(),device["actions"].end());
    return {{"devices",devices},{"limit",limit},{"offset",offset}};
}
}

namespace spark {
json FileStore::layout() const {
    Connection c(config_);Statement q(c,"SELECT JSON_OBJECT('id',id,'directory',directory,'drive_id',drive_id,'state',state,'content_revision',content_revision,'size_bytes',size_bytes) FROM files");q.execute();return q.rows();
}
std::optional<std::string> FileStore::location(const std::string& id) const {
    Connection c(config_);Statement q(c,"SELECT JSON_OBJECT('drive_id',drive_id) FROM files WHERE id=?",{id});q.execute();auto rows=q.rows();
    return rows.empty()||rows[0]["drive_id"].is_null()?std::nullopt:std::optional<std::string>(rows[0]["drive_id"].get<std::string>());
}
void FileStore::register_drives(const std::vector<std::string>& drives,const json& assignments,const json& roots,bool anchor) const {
    Connection c(config_);Transaction tx(c);
    Statement lock(c,"SELECT JSON_ARRAY(id) FROM storage_registry_lock WHERE id=1 FOR UPDATE");lock.execute();if(lock.rows().empty())throw DatabaseError("Storage registry lock missing; run setup-db.sh");
    Statement known(c,"SELECT JSON_QUOTE(id) FROM storage_drives");known.execute();const auto known_rows=known.rows();

    for(const auto& id:drives){Statement q(c,"INSERT IGNORE INTO storage_drives(id) VALUES (?)",{id});q.execute();}
    Statement numbers(c,"SELECT JSON_ARRAY(COALESCE(MAX(ordinal),0)) FROM storage_drives");numbers.execute();unsigned next=numbers.rows()[0][0].get<unsigned>();
    std::vector<std::string> ordered=drives;for(const auto& id:known_rows)if(std::find(ordered.begin(),ordered.end(),id.get<std::string>())==ordered.end())ordered.push_back(id);
    for(const auto& id:ordered){
        Statement current(c,"SELECT JSON_OBJECT('letter',letter) FROM storage_drives WHERE id=?",{id});current.execute();
        if(current.rows()[0]["letter"].is_null()){
            unsigned n=++next;std::string letter;while(n){--n;letter.insert(letter.begin(),char('A'+n%26));n/=26;}
            Statement q(c,"UPDATE storage_drives SET ordinal=?,letter=? WHERE id=?",{std::to_string(next),letter,id});q.execute();
        }
    }
    bool changed=false;for(const auto& root:roots){Statement q(c,"UPDATE storage_drives SET root_path=?,attached=TRUE WHERE id=?",{root["path"].get<std::string>(),root["id"].get<std::string>()});q.execute();changed=changed||q.changed();}
    if(changed)append_event(c,"drives.updated","/files/drives",nullptr,json::object());
    if(anchor&&!drives.empty()){
        Statement exists(c,"SELECT JSON_ARRAY(COUNT(*)) FROM storage_drives WHERE persistent=TRUE");exists.execute();
        if(exists.rows()[0][0]==0){Statement q(c,"UPDATE storage_drives SET persistent=TRUE WHERE id=?",{drives[0]});q.execute();}
    }
    for(const auto& item:assignments.items()){Statement q(c,"UPDATE files SET drive_id=? WHERE id=? AND drive_id IS NULL",{item.value().get<std::string>(),item.key()});q.execute();}
    // Finish legacy assignment once the persistent identity exists (also on fresh installs).
    Statement home(c,"SELECT JSON_QUOTE(id) FROM storage_drives WHERE persistent=TRUE");home.execute();auto homes=home.rows();
    if(!homes.empty()){
        const auto drive=homes[0].get<std::string>();
        Statement copy(c,"INSERT IGNORE INTO file_directories(drive_id,path) SELECT ?,path FROM file_directories WHERE drive_id=''",{drive});copy.execute();
        Statement clear(c,"DELETE FROM file_directories WHERE drive_id=''");clear.execute();
        Statement projects(c,"UPDATE projects SET drive_id=? WHERE drive_id IS NULL",{drive});projects.execute();
    }
    for(const auto& id:drives)ensure_directories(c,"/",id);
    for(const auto& item:assignments.items()){
        Statement read(c,"SELECT JSON_QUOTE(directory) FROM files WHERE id=?",{item.key()});read.execute();auto rows=read.rows();if(!rows.empty())ensure_directories(c,rows[0].get<std::string>(),item.value().get<std::string>());
    }
    Statement references(c,"SELECT JSON_OBJECT('drive_id',drive_id) FROM files GROUP BY drive_id");references.execute();
    for(const auto& row:references.rows())if(row["drive_id"].is_null()||std::find(ordered.begin(),ordered.end(),row["drive_id"].get<std::string>())==ordered.end())throw DatabaseError("File references unavailable storage drive");
    tx.commit();
}
}

namespace spark {
json FileStore::drives() const {
    Connection c(config_);Statement q(c,"SELECT JSON_OBJECT('id',id,'letter',letter,'path',root_path,'attached',IF(attached,TRUE,FALSE),'persistent',IF(persistent,TRUE,FALSE)) FROM storage_drives ORDER BY ordinal,id");q.execute();return q.rows();
}
void FileStore::detach_drive(const std::string& id) const {
    Connection c(config_);Transaction tx(c);Statement q(c,"UPDATE storage_drives SET attached=FALSE WHERE id=? AND persistent=FALSE",{id});q.execute();
    if(q.changed())append_event(c,"drives.updated","/files/drives",nullptr,json::object());tx.commit();
}
}
