#include "book.hpp"
#include <cassert>
#include <iostream>

int main() {
    using namespace pm;
    Book b;
    b.snapshot({{Side::bid, 4000, 100}, {Side::ask, 6000, 200}});
    b.update({{Side::bid, 4000, -100, Operation::add}, {Side::bid, 5000, 300}});
    assert(b.best_bid() == 5000);
    auto before = b.depth(Side::bid, 10001);
    try { b.update({{Side::bid, 7000, 1}}); assert(false); } catch (const std::invalid_argument&) {}
    assert(b.depth(Side::bid, 10001) == before);
    b.snapshot({{Side::bid, 0, 1}, {Side::ask, 10000, 1}});
    assert(b.best_bid() == 0 && b.best_ask() == 10000);
    b.snapshot({{Side::bid, 63, 1}, {Side::bid, 64, 2}, {Side::bid, 9999, 3}});
    assert(b.depth(Side::bid, 3)[1].first == 64);
    b.update({{Side::bid, 9999, 0}});
    assert(b.best_bid() == 64);
    b.snapshot({{Side::bid, 1, std::numeric_limits<std::int64_t>::max()}});
    try { b.update({{Side::bid, 1, 1, Operation::add}}); assert(false); }
    catch (const std::overflow_error&) {}
    try { b.update({{Side::ask, 2, std::numeric_limits<std::int64_t>::min(), Operation::add}}); assert(false); }
    catch (const std::invalid_argument&) {}
    std::cout << "C++ book checks passed\n";
}
