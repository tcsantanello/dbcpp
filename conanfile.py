# BSD 2-Clause License
#
# Copyright (c) 2020, Thomas Santanello
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from conans import ConanFile, tools, CMake

def uniq(*args):
    entries = []
    for arg in args:
        if isinstance(arg, list):
            for e in arg:
                if e not in entries:
                    entries.append(e)
        else:
            if arg not in entries:
                entries.append(arg)
    return entries

class DbcppConan(ConanFile):
    name = "dbcpp"
    license = "BSD-2-Clause"
    author = "Thomas Santanello <tcsantanello@gmail.com>"
    url = "https://gogs.apps.nemesus.homeip.net/c-cpp-projects/dbcpp.git"
    description = "Unified Database abstraction for C++"
    topics = ("database")
    build_policy = "missing"
    settings = "os", "compiler", "build_type", "arch"
    generators = "cmake"
    options = {
        "shared": [True, False],
        "with_pq": [True, False],
        "with_sqlite3": [True, False],
    }
    default_options = {
        "shared": True,
        "with_pq": True,
        "with_sqlite3": True,
    }

    def requirements(self):
        self.requires.add("boost/1.68.0@conan/stable")
        self.requires.add("spdlog/1.3.0@bincrafters/stable")
        self.requires.add("cppuri/1.0.0@tcsantanello/stable")
        if self.options.with_pq:
            self.requires.add("libpq/11.5@bincrafters/stable")
        if self.options.with_sqlite3:
            self.requires.add("sqlite3/3.29.0@bincrafters/stable")

    def export_sources(self):
        git = tools.Git(".")
        status_files =[x[2:].strip() for x in git.run("status -s --untracked-files=no").split("\n")]
        checkout_files = git.run("ls-tree -r HEAD --name-only").split("\n")
        files = uniq(status_files, checkout_files)
        for file in files:
            self.copy(file)

    def build(self):
        cmake = CMake(self)
        cmake.verbose = True
        if self.options.with_pq:
            cmake.definitions["POSTGRESQL_ENABLE"] = "ON"
        if self.options.with_sqlite3:
            cmake.definitions["SQLITE3_ENABLE"] = "ON"
        cmake.configure()
        cmake.build()

    def package(self):
        for suffix in ["h", "hh", "hpp", "H", "hxx", "hcc"]:
            self.copy("*.{}".format(suffix))
        self.copy("*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.dylib*", dst="lib", keep_path=False)
        self.copy("*.so*", dst="lib", keep_path=False, symlinks=True)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)
