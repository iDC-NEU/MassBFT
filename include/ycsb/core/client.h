//
// Created by peng on 10/18/22.
//

#ifndef BENCHMARKCLIENT_CLIENT_H
#define BENCHMARKCLIENT_CLIENT_H

#include <vector>
#include <thread>
#include <string>
#include <cassert>
#include <bthread/countdown_event.h>
#include "lightweightsemaphore.h"
#include "yaml-cpp/yaml.h"
#include "glog/logging.h"
#include "ycsb/core/workload/workload.h"
#include "ycsb/core/workload/core_workload.h"
#include "ycsb/core/db_factory.h"
#include "client_thread.h"

namespace ycsb::core {
    class Client {
    public:
        // Use a global workload to create some worker class
        // each worker instance is assigned with a unique db instance created by dbName
        // db name: any is ok
        static
    };
}
#endif //BENCHMARKCLIENT_CLIENT_H
