#pragma once
#include <array>
#include <bit>
#include <bitset>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace pm {
constexpr int max_price = 10000;
constexpr int words = (max_price + 64) / 64;
enum class Side { bid, ask };
enum class Operation { set, add };
struct Level {
    Side side;
    int price;
    std::int64_t size;
    Operation operation = Operation::set;
};

class Occupied {
    std::array<std::uint64_t, words> bits_{};
    std::array<std::uint64_t, (words + 63) / 64> summary_{};
public:
    void set(int price, bool present) {
        auto word = price / 64;
        auto mask = std::uint64_t{1} << (price % 64);
        if (present) bits_[word] |= mask;
        else bits_[word] &= ~mask;
        auto summary_mask = std::uint64_t{1} << (word % 64);
        if (bits_[word]) summary_[word / 64] |= summary_mask;
        else summary_[word / 64] &= ~summary_mask;
    }
    std::optional<int> best(bool highest) const {
        for (int i = 0; i < static_cast<int>(summary_.size()); ++i) {
            auto s = highest ? static_cast<int>(summary_.size()) - 1 - i : i;
            if (!summary_[s]) continue;
            auto w = s * 64 + (highest ? 63 - std::countl_zero(summary_[s])
                                      : std::countr_zero(summary_[s]));
            auto b = highest ? 63 - std::countl_zero(bits_[w]) : std::countr_zero(bits_[w]);
            return w * 64 + b;
        }
        return std::nullopt;
    }
};

class Book {
    std::array<std::int64_t, max_price + 1> bids_{};
    std::array<std::int64_t, max_price + 1> asks_{};
    Occupied bid_bits_, ask_bits_;

    void assign(Side side, int price, std::int64_t size) {
        (side == Side::bid ? bids_ : asks_)[price] = size;
        (side == Side::bid ? bid_bits_ : ask_bits_).set(price, size != 0);
    }
    static void validate_levels(const std::vector<Level>& levels, bool snapshot) {
        std::bitset<2 * (max_price + 1)> seen;
        for (const auto& l : levels) {
            if (l.price < 0 || l.price > max_price) throw std::invalid_argument("price_out_of_range");
            if (l.side != Side::bid && l.side != Side::ask) throw std::invalid_argument("invalid_side");
            if (l.operation != Operation::set && l.operation != Operation::add)
                throw std::invalid_argument("invalid_operation");
            if ((snapshot || l.operation == Operation::set) && l.size < 0)
                throw std::invalid_argument("negative_size");
            if (snapshot && l.operation != Operation::set) throw std::invalid_argument("snapshot_requires_set");
            auto index = l.price + (l.side == Side::ask ? max_price + 1 : 0);
            if (seen[index]) throw std::invalid_argument("duplicate_level");
            seen[index] = true;
        }
    }
    bool crossed() const {
        auto bid = best_bid(), ask = best_ask();
        return bid && ask && *bid >= *ask;
    }
public:
    std::optional<int> best_bid() const { return bid_bits_.best(true); }
    std::optional<int> best_ask() const { return ask_bits_.best(false); }
    std::int64_t quantity(Side side, int price) const {
        if (price < 0 || price > max_price) throw std::invalid_argument("price_out_of_range");
        if (side != Side::bid && side != Side::ask) throw std::invalid_argument("invalid_side");
        return (side == Side::bid ? bids_ : asks_)[price];
    }
    void snapshot(const std::vector<Level>& levels) {
        validate_levels(levels, true);
        Book next;
        for (const auto& l : levels) next.assign(l.side, l.price, l.size);
        if (next.crossed()) throw std::invalid_argument("crossed_book");
        *this = std::move(next);
    }
    void update(const std::vector<Level>& levels) {
        validate_levels(levels, false);
        std::vector<Level> resolved;
        resolved.reserve(levels.size());
        for (auto l : levels) {
            auto old = quantity(l.side, l.price);
            if (l.operation == Operation::add) {
                if (l.size > 0 && old > std::numeric_limits<std::int64_t>::max() - l.size)
                    throw std::overflow_error("size_overflow");
                if (l.size < 0 && l.size < -old) throw std::invalid_argument("negative_result");
                l.size += old;
            }
            resolved.push_back(l);
        }
        std::vector<Level> undo;
        undo.reserve(resolved.size());
        for (const auto& l : resolved) {
            undo.push_back({l.side, l.price, quantity(l.side, l.price)});
            assign(l.side, l.price, l.size);
        }
        if (crossed()) {
            for (const auto& l : undo) assign(l.side, l.price, l.size);
            throw std::invalid_argument("crossed_book");
        }
    }
    std::vector<std::pair<int, std::int64_t>> depth(Side side, std::size_t count) const {
        if (side != Side::bid && side != Side::ask) throw std::invalid_argument("invalid_side");
        auto occupied = side == Side::bid ? bid_bits_ : ask_bits_;
        const auto& quantities = side == Side::bid ? bids_ : asks_;
        std::vector<std::pair<int, std::int64_t>> out;
        out.reserve(std::min(count, static_cast<std::size_t>(max_price + 1)));
        while (out.size() < count) {
            auto price = occupied.best(side == Side::bid);
            if (!price) break;
            out.emplace_back(*price, quantities[*price]);
            occupied.set(*price, false);
        }
        return out;
    }
};
}
