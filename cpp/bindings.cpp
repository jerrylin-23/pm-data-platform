#include "book.hpp"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
namespace py = pybind11;

PYBIND11_MODULE(_book, m) {
    m.doc() = "Pure C++20 integer price-level order book";
    py::enum_<pm::Side>(m, "Side").value("BID", pm::Side::bid).value("ASK", pm::Side::ask);
    py::enum_<pm::Operation>(m, "Operation")
        .value("SET_SIZE", pm::Operation::set).value("ADD_SIZE", pm::Operation::add);
    py::class_<pm::Level>(m, "Level")
        .def(py::init<pm::Side, int, std::int64_t, pm::Operation>(),
             py::arg("side"), py::arg("price"), py::arg("size"), py::arg("operation") = pm::Operation::set);
    py::class_<pm::Book>(m, "Book")
        .def(py::init<>())
        .def("snapshot", &pm::Book::snapshot, py::call_guard<py::gil_scoped_release>())
        .def("update", &pm::Book::update, py::call_guard<py::gil_scoped_release>())
        .def("best_bid", &pm::Book::best_bid)
        .def("best_ask", &pm::Book::best_ask)
        .def("quantity", &pm::Book::quantity)
        .def("depth", &pm::Book::depth, py::arg("side"), py::arg("count") = 10)
        .def("clone", [](const pm::Book& b) { return pm::Book(b); });
}
