from setuptools import setup
from pybind11.setup_helpers import Pybind11Extension, build_ext

setup(
    ext_modules=[Pybind11Extension("pmplatform._book", ["cpp/bindings.cpp"], cxx_std=20,
                                  depends=["cpp/book.hpp"])],
    cmdclass={"build_ext": build_ext},
)
