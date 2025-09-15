/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef THESTRAL_PLUGIN_MASTER_UTILS_H
#define THESTRAL_PLUGIN_MASTER_UTILS_H

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>
#include <cstdlib>
#include <string>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

constexpr char kSep = '/';

static std::string GenerateUUID() {
    boost::uuids::random_generator generator;
    return boost::uuids::to_string(generator());
}

std::string MakeRandomName(int num_chars) {
    static const std::string chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string resBuf = "";
    char tmp;
    for (int i = 0; i < num_chars; i++) {
        tmp = chars[random() % (chars.length())];
        resBuf += tmp;
    }
    return resBuf;
}

std::string MakeTemporaryDir(const std::string& prefix) {
    const int kNumChars = 8;
    std::string suffix = MakeRandomName(kNumChars);
    return prefix + suffix;
}

std::vector<std::string> GetConfiguredLocalDirs() {
    auto joined_dirs_c = std::getenv("NATIVESQL_SPARK_LOCAL_DIRS");
    std::vector<std::string> res;
    if (joined_dirs_c != nullptr && strcmp(joined_dirs_c, "") > 0) {
        auto joined_dirs = std::string(joined_dirs_c);
        std::string delimiter = ",";

        size_t pos;
        while ((pos = joined_dirs.find(delimiter)) != std::string::npos) {
            auto dir = joined_dirs.substr(0, pos);
            if (dir.length() > 0) {
                res.push_back(std::move(dir));
            }
            joined_dirs.erase(0, pos + delimiter.length());
        }
        if (joined_dirs.length() > 0) {
            res.push_back(std::move(joined_dirs));
        }
        return res;
    } else {
        auto omni_tmp_dir = MakeTemporaryDir("columnar-shuffle-");
        if (!IsFileExist(omni_tmp_dir.c_str())) {
            mkdir(omni_tmp_dir.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);
        }
        return std::vector<std::string>{omni_tmp_dir};
    }
}

std::string EnsureTrailingSlash(const std::string& v) {
    if (v.length() > 0 && v.back() != kSep) {
        // XXX How about "C:" on Windows? We probably don't want to turn it into "C:/"...
        // Unless the local filesystem always uses absolute paths
        return std::string(v) + kSep;
    } else {
        return std::string(v);
    }
}

std::string RemoveLeadingSlash(std::string key) {
    while (!key.empty() && key.front() == kSep) {
        key.erase(0);
    }
    return key;
}

std::string ConcatAbstractPath(const std::string& base, const std::string& stem) {
    if(stem.empty()) {
        throw std::runtime_error("stem empty! ");
    }

    if (base.empty()) {
        return stem;
    }
    return EnsureTrailingSlash(base) + std::string(RemoveLeadingSlash(stem));
}

std::string GetSpilledShuffleFileDir(const std::string& configured_dir,
                                            int32_t sub_dir_id) {
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(2) << std::hex << sub_dir_id;
    auto dir = ConcatAbstractPath(configured_dir, "shuffle_" + ss.str());
    return dir;
}

std::string CreateTempShuffleFile(const std::string& dir) {
    if (dir.length() == 0) {
        throw std::runtime_error("CreateTempShuffleFile failed!");
    }

    if (!IsFileExist(dir.c_str())) {
        mkdir(dir.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);
    }

    std::string file_path =  ConcatAbstractPath(dir, "temp_shuffle_" + GenerateUUID());
    return file_path;
}

#endif //THESTRAL_PLUGIN_MASTER_UTILS_H
