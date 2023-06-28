//
// Created by peng on 11/6/22.
//

#include "ycsb/core/status_thread.h"
#include "ycsb/core/client_thread.h"
#include "ycsb/core/workload/core_workload.h"
#include "ycsb/core/common/ycsb_property.h"

using namespace ycsb;

class YCSBEngine {
public:
    explicit YCSBEngine(const util::Properties &n) :factory(n) {
        ycsbProperties = utils::YCSBProperties::NewFromProperty(n);
        auto workload = std::make_shared<ycsb::core::workload::CoreWorkload>();
        workload->init(*ycsbProperties);
        measurements = std::make_shared<core::Measurements>();
        workload->setMeasurements(measurements);
        initClients(workload);

    }

    void startTest() {
        LOG(INFO) << "Running test.";
        auto status = factory.newDBStatus();
        ycsb::core::StatusThread statusThread(measurements, std::move(status));

        LOG(INFO) << "Run worker thread";
        for(auto &client :clients) {
            client->run();
        }
        LOG(INFO) << "Run status thread";
        statusThread.run();

        LOG(INFO) << "All worker exited";
    }

protected:
    void initClients(const std::shared_ptr<core::workload::Workload>& workload) {
        auto operationCount = ycsbProperties->getOperationCount();
        auto threadCount = std::min(ycsbProperties->getThreadCount(), (int)operationCount);
        auto threadOpCount = operationCount / threadCount + 1;
        auto tpsPerThread = ycsbProperties->getTargetTPSPerThread();
        for (int tid = 0; tid < threadCount; tid++) {   // create a set of clients
            auto db = factory.newDB();  // each client create a connection
            // TODO: optimize zmq connection
            auto t = std::make_unique<core::ClientThread>(std::move(db), workload, tid, (int)threadOpCount, tpsPerThread);
            clients.emplace_back(std::move(t));
        }
    }

private:
    core::DBFactory factory;
    std::unique_ptr<ycsb::utils::YCSBProperties> ycsbProperties;
    std::shared_ptr<core::Measurements> measurements;
    std::vector<std::unique_ptr<core::ClientThread>> clients;
};

int main(int, char *[]) {
    auto* p = util::Properties::GetProperties();
    YCSBEngine engine(*p);
    engine.startTest();
    return 0;
}