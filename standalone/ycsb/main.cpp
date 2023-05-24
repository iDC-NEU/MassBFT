//
// Created by peng on 11/6/22.
//

#include "ycsb/core/common/ycsb_property.h"
#include "ycsb/core/status_thread.h"
#include "ycsb/core/client.h"
#include "ycsb/core/measurements.h"

#include "bthread/countdown_event.h"

using namespace ycsb;

auto initDB(const utils::YCSBProperties& n, core::workload::Workload* workload) {
    auto operationCount = n.getOperationCount();
    auto threadCount = std::min(n.getThreadCount(), (int)operationCount);
    auto threadOpCount = operationCount / threadCount + 1;
    std::vector<std::pair<std::unique_ptr<core::ClientThread>, std::unique_ptr<std::thread>>> clients;
    for (int tid = 0; tid < threadCount; tid++) {   // create a set of clients
        auto db = core::DBFactory::NewDB("", n);  // each client create a connection
        // TODO: optimize zmq connection
        auto t = std::make_unique<core::ClientThread>(std::move(db), workload, (int)threadOpCount, n.getTargetTPSPerThread());
        t->setSeed(tid);
        clients.emplace_back(std::move(t), t->run());
    }
    return clients;
}

int main(int argc, char *argv[]) {
    auto property = utils::YCSBProperties::NewFromProperty();
    ycsb::core::workload::CoreWorkload workload;
    workload.init(*property);
    auto measurements = std::make_shared<core::Measurements>();
    workload.setMeasurements(measurements);

    LOG(INFO) << "Starting test.";
    auto clients = initDB(*property, &workload);
    LOG(INFO) << "Running test.";

    ycsb::core::StatusThread statusThread(measurements);
    // statusThread.run();
    return 0;
}