#include "storage/append_only_log.h"

#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace mini_sqs {
namespace {

std::string hexEncode(std::string_view value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(value.size() * 2);
    for (const unsigned char character : value) {
        encoded.push_back(digits[character >> 4]);
        encoded.push_back(digits[character & 0x0f]);
    }
    return encoded;
}

unsigned char hexValue(char character) {
    if (character >= '0' && character <= '9') {
        return static_cast<unsigned char>(character - '0');
    }
    if (character >= 'a' && character <= 'f') {
        return static_cast<unsigned char>(character - 'a' + 10);
    }
    throw std::runtime_error("invalid hex value in queue log");
}

std::string hexDecode(std::string_view value) {
    if (value.size() % 2 != 0) {
        throw std::runtime_error("invalid encoded string in queue log");
    }
    std::string decoded;
    decoded.reserve(value.size() / 2);
    for (std::size_t index = 0; index < value.size(); index += 2) {
        decoded.push_back(static_cast<char>(
            (hexValue(value[index]) << 4) | hexValue(value[index + 1])));
    }
    return decoded;
}

std::vector<std::string_view> splitRecord(const std::string& line) {
    std::vector<std::string_view> fields;
    std::size_t start = 0;
    while (true) {
        const auto separator = line.find('\t', start);
        if (separator == std::string::npos) {
            fields.emplace_back(line.data() + start, line.size() - start);
            return fields;
        }
        fields.emplace_back(line.data() + start, separator - start);
        start = separator + 1;
    }
}

std::string serializeMessage(
    std::string_view type,
    std::string_view queueName,
    const Message& message) {
    const auto createdAt = std::chrono::duration_cast<std::chrono::milliseconds>(
        message.createdAt.time_since_epoch());
    return std::string(type) + '\t' + hexEncode(queueName)
        + '\t' + hexEncode(message.messageId)
        + '\t' + hexEncode(message.body)
        + '\t' + hexEncode(message.idempotencyKey)
        + '\t' + std::to_string(createdAt.count())
        + '\t' + std::to_string(message.receiveCount);
}

Message parseMessage(const std::vector<std::string_view>& fields) {
    if (fields.size() != 7) {
        throw std::runtime_error("invalid message record in queue log");
    }
    return Message{
        .messageId = hexDecode(fields[2]),
        .body = hexDecode(fields[3]),
        .idempotencyKey = hexDecode(fields[4]),
        .createdAt = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(std::stoll(std::string(fields[5])))),
        .receiveCount = static_cast<std::uint32_t>(std::stoul(std::string(fields[6]))),
    };
}

struct ReplayState {
    std::uint32_t maxReceiveCount;
    std::chrono::milliseconds visibilityTimeout;
    std::vector<std::string> messageOrder;
    std::unordered_map<std::string, Message> outstanding;
    std::vector<Message> deadLetters;
};

}  // namespace

AppendOnlyLog::AppendOnlyLog(std::filesystem::path path): path_(std::move(path)) {}

void AppendOnlyLog::recordCreate(
    std::string_view queueName,
    std::uint32_t maxReceiveCount,
    std::chrono::milliseconds visibilityTimeout) {
    append("CREATE\t" + hexEncode(queueName)
        + '\t' + std::to_string(maxReceiveCount)
        + '\t' + std::to_string(visibilityTimeout.count()));
}

void AppendOnlyLog::recordPublish(std::string_view queueName, const Message& message) {
    append(serializeMessage("PUBLISH", queueName, message));
}

void AppendOnlyLog::recordReceive(std::string_view queueName, const Message& message) {
    append(serializeMessage("RECEIVE", queueName, message));
}

void AppendOnlyLog::recordAcknowledge(std::string_view queueName, const Message& message) {
    append("ACK\t" + hexEncode(queueName) + '\t' + hexEncode(message.messageId));
}

void AppendOnlyLog::recordRequeue(std::string_view queueName, const Message& message) {
    append(serializeMessage("REQUEUE", queueName, message));
}

void AppendOnlyLog::recordDeadLetter(std::string_view queueName, const Message& message) {
    append(serializeMessage("DLQ", queueName, message));
}

void AppendOnlyLog::append(std::string_view record) {
    std::lock_guard lock(mutex_);
    if (!path_.parent_path().empty()) {
        std::filesystem::create_directories(path_.parent_path());
    }
    std::ofstream output(path_, std::ios::app);
    output << record << '\n';
    output.flush();
    if (!output) {
        throw std::runtime_error("failed to append to queue log: " + path_.string());
    }
}

std::unordered_map<std::string, RecoveredQueue> AppendOnlyLog::replay() const {
    std::lock_guard lock(mutex_);
    std::ifstream input(path_);
    if (!input && !std::filesystem::exists(path_)) {
        return {};
    }
    if (!input) {
        throw std::runtime_error("failed to read queue log: " + path_.string());
    }

    std::unordered_map<std::string, ReplayState> states;
    std::string line;
    while (std::getline(input, line)) {
        const auto fields = splitRecord(line);
        if (fields.empty()) {
            continue;
        }
        const std::string type(fields[0]);
        if (type == "CREATE") {
            if (fields.size() != 4) {
                throw std::runtime_error("invalid CREATE record in queue log");
            }
            states.try_emplace(hexDecode(fields[1]), ReplayState{
                .maxReceiveCount = static_cast<std::uint32_t>(std::stoul(std::string(fields[2]))),
                .visibilityTimeout = std::chrono::milliseconds(std::stoll(std::string(fields[3]))),
            });
            continue;
        }

        if (fields.size() < 3) {
            throw std::runtime_error("invalid record in queue log");
        }
        const std::string queueName = hexDecode(fields[1]);
        auto state = states.find(queueName);
        if (state == states.end()) {
            throw std::runtime_error("queue event precedes CREATE record");
        }
        const std::string messageId = hexDecode(fields[2]);
        if (type == "PUBLISH") {
            auto message = parseMessage(fields);
            state->second.messageOrder.push_back(message.messageId);
            state->second.outstanding.insert_or_assign(message.messageId, std::move(message));
        } else if (type == "ACK") {
            if (fields.size() != 3) {
                throw std::runtime_error("invalid ACK record in queue log");
            }
            state->second.outstanding.erase(messageId);
        } else if (type == "RECEIVE" || type == "REQUEUE") {
            auto message = parseMessage(fields);
            if (state->second.outstanding.contains(messageId)) {
                state->second.outstanding.insert_or_assign(messageId, std::move(message));
            }
        } else if (type == "DLQ") {
            auto message = parseMessage(fields);
            state->second.outstanding.erase(messageId);
            state->second.deadLetters.push_back(std::move(message));
        } else {
            throw std::runtime_error("unknown record type in queue log: " + type);
        }
    }

    std::unordered_map<std::string, RecoveredQueue> recovered;
    for (auto& [queueName, state] : states) {
        RecoveredQueue queue{
            .maxReceiveCount = state.maxReceiveCount,
            .visibilityTimeout = state.visibilityTimeout,
            .deadLetterMessages = std::move(state.deadLetters),
        };
        for (const auto& messageId : state.messageOrder) {
            auto message = state.outstanding.find(messageId);
            if (message != state.outstanding.end()) {
                queue.availableMessages.push_back(std::move(message->second));
            }
        }
        recovered.emplace(std::move(queueName), std::move(queue));
    }
    return recovered;
}

}  // namespace mini_sqs