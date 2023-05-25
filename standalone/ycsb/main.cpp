//
// Created by peng on 11/6/22.
//

#include "ycsb/core/status_thread.h"
#include "ycsb/core/client_thread.h"
#include "ycsb/core/workload/core_workload.h"
#include "ycsb/core/common/ycsb_property.h"

using namespace ycsb;

auto initClientThreads(const utils::YCSBProperties& n, const std::shared_ptr<core::workload::Workload>& workload) {
    auto operationCount = n.getOperationCount();
    auto threadCount = std::min(n.getThreadCount(), (int)operationCount);
    auto threadOpCount = operationCount / threadCount + 1;
    auto tpsPerThread = n.getTargetTPSPerThread();
    std::vector<std::unique_ptr<core::ClientThread>> clients;
    for (int tid = 0; tid < threadCount; tid++) {   // create a set of clients
        auto db = core::DB::NewDB("", n);  // each client create a connection
        // TODO: optimize zmq connection
        auto t = std::make_unique<core::ClientThread>(std::move(db), workload, tid, (int)threadOpCount, tpsPerThread);
        clients.emplace_back(std::move(t));
    }
    return clients;
}

int main(int, char *[]) {
    auto property = utils::YCSBProperties::NewFromProperty();
    auto workload = std::make_shared<ycsb::core::workload::CoreWorkload>();
    workload->init(*property);
    auto measurements = std::make_shared<core::Measurements>();
    workload->setMeasurements(measurements);

    LOG(INFO) << "Starting test.";
    auto clients = initClientThreads(*property, workload);
    LOG(INFO) << "Running test.";

    auto db = core::DB::NewDB("", *property);  // each client create a connection
    ycsb::core::StatusThread statusThread(measurements, std::move(db));
    // TODO: run the clients and status
    return 0;
}