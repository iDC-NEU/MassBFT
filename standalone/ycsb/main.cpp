//
// Created by peng on 11/6/22.
//

#include "ycsb/core/status_thread.h"
#include "ycsb/core/client_thread.h"
#include "ycsb/core/workload/core_workload.h"
#include "ycsb/core/common/ycsb_property.h"
#include "peer/consensus/block_content/request_collector.h"

using namespace ycsb;

auto initClientThreads(const utils::YCSBProperties& n, const std::shared_ptr<core::workload::Workload>& workload) {
    auto operationCount = n.getOperationCount();
    auto threadCount = std::min(n.getThreadCount(), (int)operationCount);
    auto threadOpCount = operationCount / threadCount + 1;
    auto tpsPerThread = n.getTargetTPSPerThread();
    std::vector<std::unique_ptr<core::ClientThread>> clients;
    for (int tid = 0; tid < threadCount; tid++) {   // create a set of clients
        auto db = core::DB::NewDB("neuChain", n);  // each client create a connection
        // TODO: optimize zmq connection
        auto t = std::make_unique<core::ClientThread>(std::move(db), workload, tid, (int)threadOpCount, tpsPerThread);
        clients.emplace_back(std::move(t));
    }
    return clients;
}

int main(int, char *[]) {
    auto ycsbProperty = utils::YCSBProperties::NewFromProperty();
    auto workload = std::make_shared<ycsb::core::workload::CoreWorkload>();
    workload->init(*ycsbProperty);
    auto measurements = std::make_shared<core::Measurements>();
    workload->setMeasurements(measurements);

    LOG(INFO) << "Starting test.";
    auto clients = initClientThreads(*ycsbProperty, workload);
    LOG(INFO) << "Running test.";

    auto dbStatus = core::DBStatus::NewDBStatus("neuChain");  // each client create a connection
    ycsb::core::StatusThread statusThread(measurements, std::move(dbStatus));

    LOG(INFO) << "RequestCollector started.";
    peer::consensus::RequestCollector::Config config(100, 10);
    peer::consensus::RequestCollector collector(config, 51200);

    int totalRequests = 0;
    int totalBatches = 0;
    collector.setBatchCallback([&](const std::vector<std::unique_ptr<proto::Envelop>>& unorderedRequests) {
        LOG(INFO) << "Get a batch, size: " << unorderedRequests.size();
        totalRequests += (int)unorderedRequests.size();
        totalBatches++;
        return true;
    });
    collector.start();

    // TODO: run the clients and status
    LOG(INFO) << "all workers started";
    for(auto &client :clients){
        client->run();
    }
    LOG(INFO) << "all ClientThreads exited.";

    LOG(INFO) << "statusThread started";
    statusThread.run();

    return 0;
}